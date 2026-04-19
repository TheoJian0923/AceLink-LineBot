using Line.Messaging;
using Newtonsoft.Json;
using System.Text;
using System.Security.Cryptography;
using System.Linq; 
using System.Text.RegularExpressions; 

var builder = WebApplication.CreateBuilder(args);

#region --- 1. 核心服務配置 ---
// string accessToken = "";
// string channelSecret = "";

string accessToken = Environment.GetEnvironmentVariable("LINE_ACCESS_TOKEN") ?? "";
string channelSecret = Environment.GetEnvironmentVariable("LINE_CHANNEL_SECRET") ?? "";



builder.Services.AddSingleton<VolleyManager>();
builder.Services.AddSingleton<ILineMessagingClient>(_ => new LineMessagingClient(accessToken));
builder.Services.AddHostedService<ResetTaskService>();

var app = builder.Build();
#endregion

#region --- 2. Webhook 處理邏輯 ---
app.MapPost("/api/linebot", async (HttpContext context, ILineMessagingClient lineClient, VolleyManager manager) =>
{
    context.Request.EnableBuffering();
    context.Request.Headers.TryGetValue("X-Line-Signature", out var signatureHeader);
    string signature = signatureHeader.ToString() ?? string.Empty;
    using var reader = new StreamReader(context.Request.Body, Encoding.UTF8, leaveOpen: true); 
    var body = await reader.ReadToEndAsync();
    context.Request.Body.Position = 0;

    var key = Encoding.UTF8.GetBytes(channelSecret);
    using (var hmac = new HMACSHA256(key))
    {
        var hashBytes = hmac.ComputeHash(Encoding.UTF8.GetBytes(body));
        string computedSignature = Convert.ToBase64String(hashBytes);
        #if DEBUG
        #else
        if (computedSignature != signature) return Results.Unauthorized();
        #endif
    }

    try
    {
        dynamic? json = JsonConvert.DeserializeObject(body);
        if (json?.events == null) return Results.Ok();
        foreach (var ev in json.events)
        {
            string groupId = ev.source?.groupId ?? ev.source?.userId ?? "Default";
            var data = manager.Load(groupId); 

            string replyToken = ev.replyToken;
            string userMessage = (ev.message?.text ?? "").ToString().Replace("　", " ").Trim();
            string userId = ev.source?.userId ?? "";
            string developerId = "U4ae0a4b6b86b73455ca52ccab9ebc652";
            
            // 身份判定邏輯
            bool isDeveloper = (userId == "U4ae0a4b6b86b73455ca52ccab9ebc652");
            bool isAdmin = isDeveloper || data.Admins.ContainsKey(userId);

            if (string.IsNullOrEmpty(userMessage)) continue;

            // 1. [PlanA] 授權檢查邏輯
            if (!isDeveloper && !data.IsAuthorized)
            {
                if (userMessage != "我的ID")
                {
                    // (1) 先回覆警告訊息（Reply 是免費的）
                    await lineClient.ReplyMessageAsync(replyToken, "⚠️ \n此群組尚未授權使用。\n機器人將自動退出，如有需求請聯繫開發者。\nLine ID : 5522522333");
                    
                    // (2) 發送最後一則通知給開發者（保留追蹤線索）
                    string alertMsg = $"🚫 【自動退群通知】\n群組 ID：\n{groupId}\n內容：{userMessage}";
                    await lineClient.PushMessageAsync(developerId, alertMsg);

                    // (3) 執行自動退出邏輯（使用 HttpClient 直接呼叫 API，避免 SDK 編譯錯誤）
                    try 
                    {
                        string sourceType = ev.source?.type ?? "";
                        if (sourceType == "group" || sourceType == "room")
                        {
                            using (var httpClient = new HttpClient())
                            {
                                // 設定認證標頭
                                httpClient.DefaultRequestHeaders.Authorization = 
                                    new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", accessToken);
                                
                                // 判斷群組或聊天室路徑
                                string endpoint = sourceType == "group" 
                                    ? $"https://api.line.me/v2/bot/group/{groupId}/leave"
                                    : $"https://api.line.me/v2/bot/room/{groupId}/leave";

                                // 發送 POST 請求
                                await httpClient.PostAsync(endpoint, null);
                            }
                        }
                    }
                    catch (Exception ex) 
                    {
                        // 僅記錄錯誤，不讓退群失敗導致整個 Webhook 當掉
                        Console.WriteLine($"Leave Error: {ex.Message}");
                    }
                    
                    continue; // 終止後續邏輯執行
                }
            }

            if (userMessage == "我的ID")
            {
                string status = data.WhiteList.ContainsKey(userId) ? $"已綁定：{data.WhiteList[userId]}" : "尚未綁定";
                
                // 構造回覆訊息，明確加入群組 ID (groupId)
                var sb = new StringBuilder();
                sb.AppendLine("🆔 身份識別資訊");
                sb.AppendLine("------------------");
                sb.AppendLine($"● 您的個人 ID：\n{userId}");
                sb.AppendLine($"● 目前群組 ID：\n{groupId}"); // 這裡就是捷徑需要的 groupId
                sb.AppendLine("------------------");
                sb.AppendLine($"● 綁定狀態：{status}");
                
                await lineClient.ReplyMessageAsync(replyToken, sb.ToString().Trim());
                continue;
            }

            var lines = userMessage.Split('\n').Select(l => l.Trim()).ToList();
            string cmd = lines[0];

            #region --- 開發者指令區 ---
            var devOnlyCommands = new List<string> { "新增管理員", "移除管理員", "授權群組", "移除群組授權", "設定雲端網址", "查詢現有管理員", "查詢已授權群組", "目前設定", "取消重置時間", "開啟重置時間", "開發者指令", "清除群組資料", "確認刪除資料", "導入", "確認導入" };
            if (devOnlyCommands.Any(c => cmd.StartsWith(c)))
            {
                if (!isDeveloper) { await lineClient.ReplyMessageAsync(replyToken, "❌ 權限不足：此指令僅限開發者使用。"); continue; }

                // --- [開發者指令] 導入模板 [來源暱稱] ---
                if (cmd.StartsWith("導入"))
                {
                    if (!isDeveloper) return Results.Ok();
                    string sourceNickName = userMessage.Replace("導入", "").Trim();
                    
                    // 尋找來源 ID
                    var folderPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "GroupsData");
                    var files = Directory.GetFiles(folderPath, "*.json");
                    string? sourceId = files.Select(f => new { id = Path.GetFileNameWithoutExtension(f), d = JsonConvert.DeserializeObject<VolleyData>(File.ReadAllText(f)) })
                                            .FirstOrDefault(x => x.d?.GroupName == sourceNickName)?.id;

                    if (sourceId == null) {
                        await lineClient.ReplyMessageAsync(replyToken, $"❌ 找不到名為「{sourceNickName}」的模板來源。");
                        continue;
                    }

                    // 記錄轉移關係 (Key: 目前執行指令的開發者, Value: 來源ID|目標群組ID)
                    manager.PendingImports[userId] = $"{sourceId}|{groupId}";

                    await lineClient.ReplyMessageAsync(replyToken, 
                        $"📋 【導入模板確認】\n" +
                        $"------------------\n" +
                        $"來源模板：{sourceNickName}\n" +
                        $"套用目標：{data.GroupName} ({groupId})\n" +
                        $"------------------\n" +
                        $"⚠️ 將複製以下設定：\n" +
                        $"● 賽季時間與比賽時段\n" +
                        $"● 季打/冷氣費用與預收金額\n" +
                        $"● 完整季打名單 ({manager.Load(sourceId).MaleQuarterly.Count + manager.Load(sourceId).FemaleQuarterly.Count} 位)\n" +
                        $"● 自動重置與截止期限設定\n\n" +
                        $"注意：現有報名、對帳紀錄將清空，並開啟新 GAS 分頁。\n" +
                        $"確認請輸入：確認導入");
                    continue;
                }

                // --- [開發者指令] 確認執行導入 ---
                if (cmd == "確認導入")
                {
                    if (!isDeveloper || !manager.PendingImports.ContainsKey(userId)) return Results.Ok();

                    string[] ids = manager.PendingImports[userId].Split('|');
                    string sourceId = ids[0];
                    string targetId = ids[1];

                    try {
                        var sourceData = manager.Load(sourceId);
                        // 重要：直接操作當前 Webhook 生命週期的 data 物件，確保稍後的存檔一致
                        // 如果 targetId 跟目前的 groupId 不同，才 load 新的
                        var targetData = (targetId == groupId) ? data : manager.Load(targetId);

                        // --- 校正 A：繼承關鍵網址 ---
                        targetData.GasUrl = sourceData.GasUrl; 

                        // --- 校正 B：基礎設定賦值 ---
                        targetData.SeasonStart = sourceData.SeasonStart;
                        targetData.SeasonEnd = sourceData.SeasonEnd;
                        targetData.MatchDay = sourceData.MatchDay;
                        targetData.MatchHour = sourceData.MatchHour;
                        targetData.MatchMinute = sourceData.MatchMinute;
                        targetData.QuarterlyFee = sourceData.QuarterlyFee;
                        targetData.AcFee = sourceData.AcFee;
                        targetData.PrepaidFee = sourceData.PrepaidFee;
                        targetData.IsAcAlwaysOn = sourceData.IsAcAlwaysOn;
                        targetData.ResetDay = sourceData.ResetDay;
                        targetData.ResetHour = sourceData.ResetHour;
                        targetData.ResetMinute = sourceData.ResetMinute;
                        targetData.DeadlineDay = sourceData.DeadlineDay;
                        targetData.DeadlineHour = sourceData.DeadlineHour;
                        targetData.DeadlineMinute = sourceData.DeadlineMinute;
                        targetData.CancelDeadlineDay = sourceData.CancelDeadlineDay;
                        targetData.CancelDeadlineHour = sourceData.CancelDeadlineHour;
                        targetData.CancelDeadlineMinute = sourceData.CancelDeadlineMinute;
                        
                        targetData.MaleQuarterly = new HashSet<string>(sourceData.MaleQuarterly);
                        targetData.FemaleQuarterly = new HashSet<string>(sourceData.FemaleQuarterly);

                        // --- 校正 C：完全繼承當前動態名單 (您的理想效果) ---
                        // 這裡不再執行 ResetToQuarterly()，而是直接從來源拷貝當前所有報名狀態
                        targetData.MaleParticipants = new List<string>(sourceData.MaleParticipants);
                        targetData.FemaleParticipants = new List<string>(sourceData.FemaleParticipants);
                        targetData.MaleWaitingList = new List<string>(sourceData.MaleWaitingList);
                        targetData.FemaleWaitingList = new List<string>(sourceData.FemaleWaitingList);
                        
                        // 同步拷貝當週的特殊狀態
                        targetData.AcRecords = new Dictionary<string, bool>(sourceData.AcRecords);
                        targetData.ClosedDates = new Dictionary<string, bool>(sourceData.ClosedDates);
                        
                        targetData.IsAuthorized = true; 
                        targetData.SetupStep = 0; 
                        targetData.IsRecentlyReset = true; 

                        // 存檔
                        manager.Save(targetId, targetData);
                        manager.PendingImports.Remove(userId);

                        // --- 校正 D：非阻塞異步同步 ---
                        var now = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("Taipei Standard Time"));
                        string forceDateKey = targetData.GetCalibratedMatchDate(now).ToString("yyyyMMdd");

                        _ = Task.Run(async () => {
                            // 這裡傳入 true 觸發 GAS 生成/確認標題，同時傳入 forceDateKey 確保名單上傳
                            await targetData.SyncToSheets(lineClient, targetId, true, forceDateKey);
                        });

                        await lineClient.ReplyMessageAsync(replyToken, $"✅ 導入成功！\n群組「{targetData.GroupName}」已完全繼承「{sourceData.GroupName}」之設定與目前報名名單。\n雲端表格同步中...");
                    }
                    catch (Exception ex) {
                        await lineClient.ReplyMessageAsync(replyToken, $"❌ 導入過程出錯：{ex.Message}");
                    }
                    return Results.Ok();
                }

                // 1. 發起刪除申請：清除群組資料 [暱稱]
                if (cmd.StartsWith("清除群組資料"))
                {
                    if (!isDeveloper) return Results.Ok();
                    string targetNickName = userMessage.Replace("清除群組資料", "").Trim();
                    
                    // 遍歷實體檔案尋找匹配的暱稱
                    var folderPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "GroupsData");
                    var files = Directory.GetFiles(folderPath, "*.json");
                    
                    string? foundGId = null;
                    VolleyData? foundData = null;

                    foreach (var file in files)
                    {
                        var jsonContent = File.ReadAllText(file);
                        var d = JsonConvert.DeserializeObject<VolleyData>(jsonContent);
                        if (d?.GroupName == targetNickName)
                        {
                            foundGId = Path.GetFileNameWithoutExtension(file);
                            foundData = d;
                            break;
                        }
                    }

                    if (foundGId == null || foundData == null)
                    {
                        await lineClient.ReplyMessageAsync(replyToken, $"❌ 找不到名為「{targetNickName}」的群組資料。");
                        continue;
                    }

                    // 紀錄到記憶體暫存區
                    manager.PendingDeletes[foundGId] = targetNickName;

                    // 構造「目前設定」格式的二次確認訊息
                    var sb = new StringBuilder();
                    sb.AppendLine("⚠️ 【請確認欲刪除的群組設定】");
                    sb.AppendLine("━━━━━━━━━━━━━━━");
                    sb.AppendLine($"📍 群組名稱：{foundData.GroupName}");
                    sb.AppendLine($"🆔 群組 ID：{foundGId}");
                    sb.AppendLine("------------------");
                    sb.AppendLine($"● 授權狀態：{(foundData.IsAuthorized ? "已授權" : "未授權")}");
                    sb.AppendLine($"● 管理員人數：{foundData.Admins.Count}");
                    sb.AppendLine($"● 球季期間：{foundData.SeasonStart} ~ {foundData.SeasonEnd}");
                    sb.AppendLine($"● 比賽時間：週({foundData.MatchDay}) {foundData.MatchHour:D2}:{foundData.MatchMinute:D2}");
                    sb.AppendLine($"● 季打費用：{foundData.QuarterlyFee} 元");
                    sb.AppendLine($"● 雲端網址：{(string.IsNullOrEmpty(foundData.GasUrl) ? "未設定" : "已設定")}");
                    sb.AppendLine("━━━━━━━━━━━━━━━");
                    sb.AppendLine($"\n確認刪除請複製並輸入：\n確認刪除資料 {foundGId}");
                    sb.AppendLine("\n(若不刪除，直接輸入其他指令即可忽略此申請)");

                    await lineClient.ReplyMessageAsync(replyToken, sb.ToString().Trim());
                    continue;
                }

                // 2. 最終確認刪除：確認刪除資料 [ID]
                if (cmd.StartsWith("確認刪除資料"))
                {
                    if (!isDeveloper) return Results.Ok();

                    // 使用正則表達式精準抓取 ID
                    var match = Regex.Match(userMessage, @"確認刪除資料\s*([a-zA-Z0-9]+)");
                    if (!match.Success) 
                    {
                        await lineClient.ReplyMessageAsync(replyToken, "❌ 指令格式錯誤，請提供正確的 ID。");
                        return Results.Ok();
                    }

                    string targetId = match.Groups[1].Value.Trim();

                    if (manager.PendingDeletes.ContainsKey(targetId))
                    {
                        string groupName = manager.PendingDeletes[targetId];
                        string filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "GroupsData", $"{targetId}.json");

                        if (File.Exists(filePath))
                        {
                            try 
                            {
                                File.Delete(filePath);
                                manager.PendingDeletes.Remove(targetId);
                                await lineClient.ReplyMessageAsync(replyToken, $"🗑️ 刪除成功！已永久移除「{groupName}」的資料檔案。");
                                
                                // 成功後立即中斷，防止執行後續可能的存檔動作
                                return Results.Ok(); 
                            }
                            catch (Exception ex) 
                            {
                                await lineClient.ReplyMessageAsync(replyToken, $"❌ 刪除檔案時發生錯誤：{ex.Message}");
                            }
                        }
                        else 
                        {
                            await lineClient.ReplyMessageAsync(replyToken, "❌ 找不到實體檔案，可能已被移除。");
                            manager.PendingDeletes.Remove(targetId);
                        }
                    }
                    else 
                    {
                        await lineClient.ReplyMessageAsync(replyToken, "❌ 找不到申請紀錄，請重新輸入「清除群組資料 [暱稱]」。");
                    }
                    return Results.Ok(); 
                }       

                if (cmd == "開發者指令")
                {
                    var sb = new StringBuilder();
                    sb.AppendLine("⚡ 【AceLink 開發者控制台】 ⚡");
                    sb.AppendLine("━━━━━━━━━━━━━━━");
                    sb.AppendLine("🔑 [ 授權與權限 ]");
                    sb.AppendLine("● 授權群組 [暱稱] [群組ID]");
                    sb.AppendLine("● 移除群組授權 [群組ID]");
                    sb.AppendLine("● 新增管理員 [暱稱] [ID]");
                    sb.AppendLine("● 移除管理員 [ID]");
                    sb.AppendLine("● 清除群組資料 [群組暱稱]");
                    sb.AppendLine("● 導入 [群組暱稱]");
                    sb.AppendLine("");
                    sb.AppendLine("📊 [ 狀態監控 ]");
                    sb.AppendLine("● 目前設定 (查當前群組細節)");
                    sb.AppendLine("● 查詢已授權群組");
                    sb.AppendLine("● 查詢現有管理員");
                    sb.AppendLine("● 我的ID (查用戶及群組ID)");
                    sb.AppendLine("");
                    sb.AppendLine("⚙️ [ 進階控制 ]");
                    sb.AppendLine("● 設定雲端網址 [URL]");
                    sb.AppendLine("● 取消重置時間 (停止自動重置)");
                    sb.AppendLine("● 開啟重置時間 (恢復自動重置)");
                    sb.AppendLine("━━━━━━━━━━━━━━━");
                    sb.AppendLine("⚠️ 開發者指令具備最高權限，請謹慎操作。");

                    await lineClient.ReplyMessageAsync(replyToken, sb.ToString().Trim());
                    continue;
                }                
                
                if (cmd == "目前設定")
                {
                    var sb = new StringBuilder();
                    sb.AppendLine("⚙️ 當前群組設定參數");
                    sb.AppendLine("------------------");
                    sb.AppendLine($"● 授權狀態：{(data.IsAuthorized ? "已授權" : "未授權")}");
                    sb.AppendLine($"● 管理員人數：{data.Admins.Count}");
                    sb.AppendLine($"● 球季期間：{data.SeasonStart} ~ {data.SeasonEnd}");
                    sb.AppendLine($"● 比賽時間：週({data.MatchDay}) {data.MatchHour:D2}:{data.MatchMinute:D2}");
                    sb.AppendLine($"● 季打費用：{data.QuarterlyFee} 元");
                    sb.AppendLine($"● 冷氣費用：{data.AcFee} 元");
                    sb.AppendLine($"● 重置時間：週({data.ResetDay}) {data.ResetHour:D2}:{data.ResetMinute:D2}");
                    sb.AppendLine($"● 報名期限：{(data.DeadlineDay.HasValue ? $"週({data.DeadlineDay}) {data.DeadlineHour:D2}:{data.DeadlineMinute:D2}" : "未設定")}");
                    sb.AppendLine($"● 取消期限：{(data.CancelDeadlineDay.HasValue ? $"週({data.CancelDeadlineDay}) {data.CancelDeadlineHour:D2}:{data.CancelDeadlineMinute:D2}" : "未設定")}");
                    sb.AppendLine($"● 雲端網址：{(string.IsNullOrEmpty(data.GasUrl) ? "未設定" : "已設定")}");
                    sb.AppendLine($"● 冷氣模式：{(data.IsAcAlwaysOn ? "保持開啟" : "保持關閉")}");
                    sb.AppendLine("------------------");
                    sb.AppendLine($"● 季打男：{data.MaleQuarterly.Count} 位");
                    sb.AppendLine($"● 季打女：{data.FemaleQuarterly.Count} 位");
                    
                    await lineClient.ReplyMessageAsync(replyToken, sb.ToString().Trim()); 
                    continue;
                }

                if (cmd.StartsWith("新增管理員"))
                {
                    var parts = userMessage.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length >= 3)
                    {
                        string nickName = parts[1];
                        string targetId = parts[2];
                        data.Admins[targetId] = nickName;
                        manager.Save(groupId, data);
                        await lineClient.ReplyMessageAsync(replyToken, $"✅ 已設為管理員：\n👤 暱稱：{nickName}\n🆔 ID：{targetId}");
                    }
                    else { await lineClient.ReplyMessageAsync(replyToken, "⚠️ 格式錯誤：新增管理員 [暱稱] 使用者ID"); }
                    continue;
                }

                if (cmd.StartsWith("移除管理員"))
                { 
                    string targetId = userMessage.Replace("移除管理員", "").Trim();
                    if (data.Admins.ContainsKey(targetId))
                    {
                        string nickName = data.Admins[targetId];
                        data.Admins.Remove(targetId);
                        manager.Save(groupId, data);
                        await lineClient.ReplyMessageAsync(replyToken, $"✅ 已移除管理員權限：\n👤 暱稱：{nickName}\n🆔 ID：{targetId}");
                    }
                    else 
                    {
                        await lineClient.ReplyMessageAsync(replyToken, $"❌ 移除失敗：找不到該管理員 ID。\n請確認 ID 是否正確或輸入「查詢現有管理員」確認。");
                    }
                    continue;
                }

                if (cmd.StartsWith("授權群組"))
                {
                    var parts = userMessage.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length >= 3)
                    {
                        string gNickName = parts[1];
                        string targetGroup = parts[2];
                        var targetData = manager.Load(targetGroup);
                        targetData.IsAuthorized = true;
                        targetData.GroupName = gNickName;
                        manager.Save(targetGroup, targetData);
                        await lineClient.ReplyMessageAsync(replyToken, $"✅ 授權群組成功：\n🏐 名稱：{gNickName}\n🆔 ID：{targetGroup}");
                    }
                    else { await lineClient.ReplyMessageAsync(replyToken, "⚠️ 格式錯誤：授權群組 [暱稱] 群組ID"); }
                    continue;
                }

                if (cmd.StartsWith("移除群組授權"))
                {
                    string targetGroup = userMessage.Replace("移除群組授權", "").Trim();
                    if (!string.IsNullOrEmpty(targetGroup))
                    {
                        var targetData = manager.Load(targetGroup);
                        targetData.IsAuthorized = false;
                        targetData.Admins.Clear(); // 撤銷授權時，同步清空該群組管理員
                        manager.Save(targetGroup, targetData);
                        await lineClient.ReplyMessageAsync(replyToken, $"🚫 已成功移除該群組授權並清空管理員名單：\n{targetGroup}");
                    }
                    continue;
                }                

                if (cmd.StartsWith("設定雲端網址"))
                {
                    string url = userMessage.Replace("設定雲端網址", "").Trim();
                    if (Uri.IsWellFormedUriString(url, UriKind.Absolute))
                    {
                        data.GasUrl = url;
                        manager.Save(groupId, data);
                        await lineClient.ReplyMessageAsync(replyToken, "✅ 雲端 GAS 網址設定成功！\n現在管理員可以進行初始化了。");
                    }
                    else { await lineClient.ReplyMessageAsync(replyToken, "❌ 網址格式錯誤，請輸入完整的 https://... 網址"); }
                    continue;
                }
                if (cmd == "查詢現有管理員")
                {
                    var files = Directory.GetFiles(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "GroupsData"), "*.json");
                    var sb = new StringBuilder("👥 全域管理員清單\n");
                    sb.AppendLine("━━━━━━━━━━━━━━");
                    foreach (var file in files)
                    {
                        string gId = Path.GetFileNameWithoutExtension(file);
                        var d = manager.Load(gId);
                        if (d.Admins.Any())
                        {
                            string gName = string.IsNullOrEmpty(d.GroupName) ? "(未命名群組)" : d.GroupName;
                            sb.AppendLine($"📍 {gName}");
                            foreach (var admin in d.Admins) 
                            {
                                sb.AppendLine($"  - {admin.Value} ({admin.Key})");
                            }
                            sb.AppendLine();
                        }
                    }
                    await lineClient.ReplyMessageAsync(replyToken, sb.ToString().Trim());
                    continue;
                }

                if (cmd == "查詢已授權群組")
                {
                    var files = Directory.GetFiles(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "GroupsData"), "*.json");
                    var sb = new StringBuilder("🔓 已授權群組清單\n");
                    sb.AppendLine("━━━━━━━━━━━━━━");
                    int count = 0;
                    foreach (var file in files)
                    {
                        string gId = Path.GetFileNameWithoutExtension(file);
                        var d = manager.Load(gId);
                        if (d.IsAuthorized)
                        {
                            count++;
                            string gName = string.IsNullOrEmpty(d.GroupName) ? "(未命名)" : d.GroupName;
                            sb.AppendLine($"{count}. {gName}");
                            sb.AppendLine($"   ID: {gId}");
                        }
                    }
                    if (count == 0) sb.Append("(目前無授權群組)");
                    await lineClient.ReplyMessageAsync(replyToken, sb.ToString().Trim());
                    continue;
                }

                if (cmd == "取消重置時間")
                {
                    data.IsResetEnabled = false;
                    manager.Save(groupId, data);
                    await lineClient.ReplyMessageAsync(replyToken, "🚫 已取消本群組的自動重置功能。\n（如需恢復請輸入「開啟重置時間」）");
                    continue;
                }

                if (cmd == "開啟重置時間")
                {
                    data.IsResetEnabled = true;
                    manager.Save(groupId, data);
                    await lineClient.ReplyMessageAsync(replyToken, "✅ 已重新開啟自動重置功能。");
                    continue;
                }                
            }
            #endregion

            #region --- 管理員指令區 ---
            var adminCommands = new List<string> { 
                "重置", "確認重置", "系統初始化", "管理員指令", "設定季打費用", "設定冷氣費用", 
                "設定季打時間", "設定重置時間", "設定報名期限", "設定取消期限", "移除報名期限", 
                "移除取消期限", "增加季打", "更新季打成員", "移除季打", "修改季打成員名稱", 
                "查詢季打", "增加報名", "取消報名" 
            };
            bool isAdminCmd = adminCommands.Contains(cmd) || 
                              Regex.IsMatch(userMessage, @"^(\d{8})\s*(開冷氣|關冷氣|無開場|有開場)$") ||
                              data.SetupStep > 0 || data.ConfirmReset;

            if (isAdminCmd)
            {
                if (!isAdmin && !isDeveloper) { await lineClient.ReplyMessageAsync(replyToken, "❌ 權限不足：此指令僅限管理員使用。"); continue; }

                // 1. 先處理「已經在等待確認」的情況
                if (data.ConfirmReset)
                {
                    if (userMessage.Contains("取消"))
                    {
                        data.ConfirmReset = false;
                        manager.Save(groupId, data); // 🚩 狀態改變要存檔
                        await lineClient.ReplyMessageAsync(replyToken, "❌ 已取消重置，資料未變動。");
                    }
                    else if (userMessage == "確認重置") // 🚩 建議直接用 userMessage 比對更準確
                    {
                        data.ConfirmReset = false;
                        data.ResetToQuarterly(); 
                        manager.Save(groupId, data); // 🚩 重置名單後要存檔
                        _ = data.SyncToSheets(lineClient, groupId);
                        await lineClient.ReplyMessageAsync(replyToken, "🧹 已完成重置：恢復季打名單並清空候補。");
                    }
                    continue; 
                }

                // 2. 處理「發起重置」的指令
                if (cmd == "重置")
                {
                    data.ConfirmReset = true;
                    manager.Save(groupId, data); // 🚩 【關鍵修正】必須先存檔，機器人才會「記得」你在等待確認
                    await lineClient.ReplyMessageAsync(replyToken, "⚠️ 【安全確認】您確定要重置嗎？\n這將恢復季打並清空候補。\n\n請在 30 秒內回覆「確認重置」\n或輸入「取消」以終止。");
                    continue;
                }

                if (cmd == "系統初始化")
                {
                    if (string.IsNullOrEmpty(data.GasUrl))
                    {
                        await lineClient.ReplyMessageAsync(replyToken, "❌ 您尚未設定雲端網址，請聯繫開發者進行設定。");
                        continue;
                    }
                    data.SetupStep = 1;
                    manager.Save(groupId, data);
                    await lineClient.ReplyMessageAsync(replyToken, "🛠️ 【AceLink 系統初始化】已啟動\n\n[Step 1/7] 設定球季期間\n請輸入起訖日期，格式如下：\n20260101\n20260331\n(或輸入「取消設定」退出)");
                    continue;
                }

                if (data.SetupStep > 0)
                {
                    if (cmd == "取消設定") { data.SetupStep = 0; manager.Save(groupId, data); await lineClient.ReplyMessageAsync(replyToken, "❌ 已退出引導設定。"); continue; }
                    if (data.SetupStep == 1)
                    {
                        if (lines.Count >= 2) {
                            data.SeasonStart = lines[0]; data.SeasonEnd = lines[1];
                            data.SetupStep = 2; manager.Save(groupId, data);
                            await lineClient.ReplyMessageAsync(replyToken, "✅ 球季期間已設定。\n\n[Step 2/7] 設定比賽與費用\n請輸入格式：\n星期 (英文)\n時間 (HHmm)\n季打費用\n冷氣費用\n\n範例：\nSaturday\n1900\n200\n40");
                        } else { await lineClient.ReplyMessageAsync(replyToken, "⚠️ 格式錯誤，請輸入球季起迄日期 (共兩行)。"); }
                        continue;
                    }
                    if (data.SetupStep == 2)
                    {
                        if (lines.Count >= 4 && Enum.TryParse<DayOfWeek>(lines[0], true, out var day)) {
                            data.MatchDay = day;
                            string timeStr = lines[1];
                            if (timeStr.Length == 4 && int.TryParse(timeStr.Substring(0, 2), out int h) && int.TryParse(timeStr.Substring(2), out int m)) {
                                data.MatchHour = h; data.MatchMinute = m;
                                data.QuarterlyFee = int.Parse(lines[2]); data.AcFee = int.Parse(lines[3]);
                                data.SetupStep = 3; manager.Save(groupId, data);
                                await lineClient.ReplyMessageAsync(replyToken, "✅ 費用與時間已設定。\n\n[Step 3/7] 設定提前收費金額\n請輸入本季預計收取的總額（例如 3000）");
                            }
                        } else { await lineClient.ReplyMessageAsync(replyToken, "⚠️ 格式錯誤，請檢查星期與費用格式。"); }
                        continue;
                    }
                    if (data.SetupStep == 3) {
                        if (int.TryParse(cmd, out int prepaid)) {
                            data.PrepaidFee = prepaid;
                            data.SetupStep = 4; manager.Save(groupId, data);
                            await lineClient.ReplyMessageAsync(replyToken, "✅ 提前收費已設定。\n\n[Step 4/7] 設定冷氣模式\n請輸入：保持開啟 或 保持關閉");
                        } else {
                            await lineClient.ReplyMessageAsync(replyToken, "⚠️ 請輸入有效的數字金額。");
                        }
                        continue;
                    }                    
                    if (data.SetupStep == 4)
                    {
                        if (cmd == "保持開啟" || cmd == "保持關閉") {
                            data.IsAcAlwaysOn = (cmd == "保持開啟");
                            data.SetupStep = 5; manager.Save(groupId, data);
                            await lineClient.ReplyMessageAsync(replyToken, $"✅ 冷氣模式已設定為：{cmd}。\n\n[Step 5/7] 匯入季打名單\n請一次性輸入性別與名單，格式如下：\n男\n小明,小李,小張\n女\n小美,小華");
                        } else { await lineClient.ReplyMessageAsync(replyToken, "⚠️ 請輸入「保持開啟」或「保持關閉」。"); }
                        continue;
                    }
                    if (data.SetupStep == 5)
                    {
                        int maleIdx = lines.IndexOf("男");
                        int femaleIdx = lines.IndexOf("女");
                        if (maleIdx != -1 && femaleIdx != -1) {
                            data.MaleQuarterly.Clear(); data.FemaleQuarterly.Clear();
                            string mNames = lines[maleIdx + 1];
                            string fNames = lines[femaleIdx + 1];
                            foreach(var n in mNames.Split(new[] { ',', '，' })) { if(!string.IsNullOrWhiteSpace(n)) data.MaleQuarterly.Add(n.Trim()); }
                            foreach(var n in fNames.Split(new[] { ',', '，' })) { if(!string.IsNullOrWhiteSpace(n)) data.FemaleQuarterly.Add(n.Trim()); }
                            data.SetupStep = 6; manager.Save(groupId, data);
                            await lineClient.ReplyMessageAsync(replyToken, "✅ 季打名單已匯入。\n\n[Step 6/7] 設定重置與取消期限\n請輸入格式：\n重置星期/時間\n取消截止星期/時間\n\n範例：\nSaturday/1200\nThursday/1500");
                        } else { await lineClient.ReplyMessageAsync(replyToken, "⚠️ 格式錯誤，請確保包含「男」與「女」標籤及名單。"); }
                        continue;
                    }
                    if (data.SetupStep == 6)
                    {
                        if (lines.Count >= 2) {
                            var p1 = lines[0].Split('/'); var p2 = lines[1].Split('/');
                            if (p1.Length == 2 && p2.Length == 2 && Enum.TryParse<DayOfWeek>(p1[0], true, out var rDay) && Enum.TryParse<DayOfWeek>(p2[0], true, out var cDay)) {
                                data.ResetDay = rDay; data.ResetHour = int.Parse(p1[1].Substring(0, 2)); data.ResetMinute = int.Parse(p1[1].Substring(2));
                                data.CancelDeadlineDay = cDay; data.CancelDeadlineHour = int.Parse(p2[1].Substring(0, 2)); data.CancelDeadlineMinute = int.Parse(p2[1].Substring(2));
                                
                                data.SetupStep = 7; manager.Save(groupId, data);

                                // --- 構造摘要訊息 ---
                                var sb = new StringBuilder("📝 【請確認新賽季設定】\n");
                                sb.AppendLine("------------------");
                                sb.AppendLine($"📅 賽季：{data.SeasonStart} ~ {data.SeasonEnd}");
                                sb.AppendLine($"⏰ 比賽：週({data.MatchDay}) {data.MatchHour:D2}:{data.MatchMinute:D2}");
                                sb.AppendLine($"💰 費用：季打 {data.QuarterlyFee} / 冷氣 {data.AcFee}");
                                sb.AppendLine($"💵 預收：{data.PrepaidFee} 元");
                                sb.AppendLine($"❄️ 冷氣：{(data.IsAcAlwaysOn ? "保持開啟" : "保持關閉")}");
                                sb.AppendLine($"👥 季打人數：男 {data.MaleQuarterly.Count} / 女 {data.FemaleQuarterly.Count}");
                                sb.AppendLine($"🔄 自動重置：週({data.ResetDay}) {data.ResetHour:D2}:{data.ResetMinute:D2}");
                                sb.AppendLine($"🚫 取消截止：週({data.CancelDeadlineDay}) {data.CancelDeadlineHour:D2}:{data.CancelDeadlineMinute:D2}");
                                sb.AppendLine("------------------");
                                sb.AppendLine("✅ 若資料正確，請輸入「確認完成」");
                                sb.AppendLine("❌ 若有誤，請輸入「取消設定」並重新開始");

                                await lineClient.ReplyMessageAsync(replyToken, sb.ToString().Trim());
                            }
                        } else {
                            await lineClient.ReplyMessageAsync(replyToken, "⚠️ 格式錯誤，請檢查日期與時間格式。");
                        }
                        continue;
                    }
                    if (data.SetupStep == 7)
                    {
                        // 如果使用者在摘要頁面選擇取消
                        if (cmd == "取消設定" || cmd == "取消") 
                        {
                            data.SetupStep = 0; 
                            manager.Save(groupId, data); 
                            await lineClient.ReplyMessageAsync(replyToken, "❌ 已取消初始化，所有變更均未生效。");
                            continue;
                        }

                        if (cmd == "確認完成") 
                        {
                            // 1. 先給予即時回覆，避免 LINE 逾時
                            await lineClient.ReplyMessageAsync(replyToken, "⏳ 正在啟動新賽季：\n1. 重置球員名單\n2. 生成雲端對帳表\n\n這可能需要 5-10 秒，請稍候...");

                            // 2. 異步執行重度任務
                            _ = Task.Run(async () => {
                                try {
                                    // 重要：先歸零步驟，防止重複觸發
                                    data.SetupStep = 0; 
                                    
                                    // 重置名單（恢復季打，清空候補與關場紀錄）
                                    data.ResetToQuarterly(); 
                                    
                                    // 存檔
                                    manager.Save(groupId, data); 
                                    
                                    // 呼叫 GAS 同步 (isNewSeason = true)
                                    await data.SyncToSheets(lineClient, groupId, true);

                                    data.IsRecentlyReset = true;//防止下次重置影響名單
                                    
                                    // 最後推播成功訊息
                                    await lineClient.PushMessageAsync(groupId, "🎊 【新賽季啟動成功！】\n✅ 雲端試算表已更新標題與預收金額。\n✅ 名單已重置為本季季打成員。\n祝本季打球愉快！");
                                } catch (Exception ex) {
                                    // 錯誤處理
                                    await lineClient.PushMessageAsync(groupId, $"❌ 系統初始化過程中發生錯誤：{ex.Message}\n請檢查 GAS 網址或網路狀態。");
                                }
                            });
                        }
                        else 
                        {
                            // 如果輸入的不是「確認完成」也不是「取消」，提醒使用者
                            await lineClient.ReplyMessageAsync(replyToken, "⚠️ 請輸入「確認完成」以啟動新賽季，或輸入「取消設定」退出。");
                        }
                        continue;
                    }
                }

                if (cmd == "管理員指令")
                {
                    string helpMsg = @"┏━🏐 AceLink 管理員中心━┓
    【 核心配置 】（ ↵ 表示需換行）
    ● 系統初始化 ➜ 
      啟動引導式流程
    ● 重置 ➜ 
      恢復當週季打名單並清空候補

    【 賽季與期限設定 】
    ● 設定季打時間並創建新表格
      設定季打時間 ↵ 
      開始日 ↵ 
      結束日 ↵ 
      星期 ↵ 
      時間 ↵ 
      預收金額
    ● 設定重置時間 ↵ 
      重置星期 ↵ 
      重置時間
    ● 設定[報名/取消]期限 ↵ 
      星期 ↵ 時間
    ● 移除[報名/取消]期限

    【 費用與日期控制 】
    ● 設定[季打/冷氣]費用 [金額]
    ● [8位日期] [無開場/有開場]
    ● [8位日期] [開冷氣/關冷氣]

    【 成員維護 】
    ● 查詢季打 ➜ 顯示季打名單
      增加季打 ↵ 性別 ↵ 姓名
      移除季打 ↵ 性別 ↵ 姓名
    ● 修改季打成員名稱 ↵ 
      舊名 ↵ 
      新名

    【 手動報名干預 】
    ● 增加報名：+1~18 性別 姓名
    ● 取消報名：-1~18 性別 姓名
┗━━━━━━━━━━━━┛";
                    await lineClient.ReplyMessageAsync(replyToken, helpMsg);
                    continue;
                }

                var acMatch = Regex.Match(userMessage, @"^(\d{8})\s*(開冷氣|關冷氣)$");
                if (acMatch.Success)
                {
                    string dateStr = acMatch.Groups[1].Value;
                    bool isOpen = acMatch.Groups[2].Value == "開冷氣";
                    data.AcRecords[dateStr] = isOpen;
                    manager.Save(groupId, data);
                    _ = data.SyncToSheets(lineClient, groupId, false, dateStr);
                    await lineClient.ReplyMessageAsync(replyToken, $"✅ 已設定 {dateStr} 為 {(isOpen ? "開冷氣" : "關冷氣")}");
                    continue;
                }

                var closeMatch = Regex.Match(userMessage, @"^(\d{8})\s*(無開場|有開場)$");
                if (closeMatch.Success)
                {
                    string dateStr = closeMatch.Groups[1].Value;
                    bool isClosed = closeMatch.Groups[2].Value == "無開場";
                    data.ClosedDates[dateStr] = isClosed;
                    manager.Save(groupId, data);
                    _ = data.SyncToSheets(lineClient, groupId, false, dateStr);
                    await lineClient.ReplyMessageAsync(replyToken, $"✅ 已設定 {dateStr} 為 {(isClosed ? "無開場 (關閉報名)" : "正常開場")}");
                    continue;
                }

                if (cmd.StartsWith("設定季打費用"))
                {
                    string valStr = Regex.Match(userMessage, @"\d+").Value;
                    if (int.TryParse(valStr, out int fee)) { data.QuarterlyFee = fee; manager.Save(groupId, data); await lineClient.ReplyMessageAsync(replyToken, $"✅ 季打費用已更新：{fee}元"); }
                    continue;
                }
                if (cmd.StartsWith("設定冷氣費用"))
                {
                    string valStr = Regex.Match(userMessage, @"\d+").Value;
                    if (int.TryParse(valStr, out int fee)) { data.AcFee = fee; manager.Save(groupId, data); await lineClient.ReplyMessageAsync(replyToken, $"✅ 冷氣費用已更新：{fee}元"); }
                    continue;
                }

                if (cmd == "設定季打時間" && lines.Count >= 6)
                {
                    var now = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("Taipei Standard Time"));
                    bool isForce = userMessage.Contains("強制更新");

                    if (!string.IsNullOrEmpty(data.SeasonEnd)) 
                    {
                        DateTime oldEnd = DateTime.ParseExact(data.SeasonEnd, "yyyyMMdd", null);
                        
                        // 核心邏輯：判定是否需要跳出警告
                        bool needsWarning = false;

                        // 1. 如果「今天」還沒到結束日 -> 需要警告
                        if (now.Date < oldEnd.Date) 
                        {
                            needsWarning = true;
                        }
                        // 2. 如果「今天」剛好是結束日，但「比賽時間」還沒到 -> 需要警告
                        else if (now.Date == oldEnd.Date)
                        {
                            // 建立一個當天比賽開始的精確時間物件
                            DateTime lastMatchStartTime = oldEnd.Date.AddHours(data.MatchHour).AddMinutes(data.MatchMinute);
                            if (now < lastMatchStartTime) 
                            {
                                needsWarning = true;
                            }
                        }

                        // 觸發警告（除非主揪已經輸入強制更新）
                        if (needsWarning && !isForce) 
                        {
                            var sbWarn = new StringBuilder();
                            sbWarn.AppendLine("⚠️ 【賽季尚未正式結束】");
                            sbWarn.AppendLine($"本季最後一場球賽預計於今日 {data.MatchHour:D2}:{data.MatchMinute:D2} 開始。");
                            sbWarn.AppendLine("現在更換賽季會封存目前的對帳表不再被更新。");
                            sbWarn.AppendLine("------------------");
                            sbWarn.AppendLine("💡 建議於球賽開始後再設定，或在指令最後加上「強制更新」四個字。");
                            
                            await lineClient.ReplyMessageAsync(replyToken, sbWarn.ToString().Trim());
                            continue; 
                        }
                    }

                    // --- 通過檢查後，執行原本的換季邏輯 ---
                    data.SeasonStart = lines[1]; 
                    data.SeasonEnd = lines[2];
                    
                    if (Enum.TryParse<DayOfWeek>(lines[3], true, out var day))
                    {
                        data.MatchDay = day;
                        string timeStr = lines[4].Replace(":", "").Trim();
                        
                        if (timeStr.Length >= 3 && int.TryParse(timeStr.Substring(0, timeStr.Length - 2), out int h) && int.TryParse(timeStr.Substring(timeStr.Length - 2), out int m))
                        {
                            data.MatchHour = h; 
                            data.MatchMinute = m;

                            if (int.TryParse(lines[5], out int prepaid))
                            {
                                data.PrepaidFee = prepaid;
                                manager.Save(groupId, data); 
                                
                                await lineClient.ReplyMessageAsync(replyToken, $"✅ 新賽季設定成功！\n期間：{data.SeasonStart}~{data.SeasonEnd}\n雲端表格已切換至新分頁。");
                                
                                _ = Task.Run(async () => {
                                    try {
                                        // 重置名單（恢復季打，清空候補）
                                        data.ResetToQuarterly(); 
                                        data.IsRecentlyReset = true;
                                        manager.Save(groupId, data);
                                        
                                        // 呼叫 GAS 同步 (isNewSeason = true)
                                        await data.SyncToSheets(lineClient, groupId, true);
                                        
                                        //await lineClient.PushMessageAsync(groupId, $"✅ 新賽季設定成功！\n期間：{data.SeasonStart}~{data.SeasonEnd}\n雲端表格已切換至新分頁。");
                                    } 
                                    catch (Exception ex) 
                                    {
                                        await lineClient.PushMessageAsync(groupId, "❌ 雲端同步失敗，請檢查網路或 GAS 設定。");
                                    }
                                });
                            }
                        }
                    }
                    continue;
                }

                if (cmd == "設定重置時間" || cmd == "設定報名期限" || cmd == "設定取消期限")
                {
                    if (lines.Count >= 3 && Enum.TryParse<DayOfWeek>(lines[1], true, out var day))
                    {
                        string timeStr = lines[2].Replace(":", "");
                        if (timeStr.Length >= 3 && int.TryParse(timeStr.Substring(0, timeStr.Length - 2), out int h) && int.TryParse(timeStr.Substring(timeStr.Length - 2), out int m))
                        {
                            if (cmd == "設定重置時間") { data.ResetDay = day; data.ResetHour = h; data.ResetMinute = m; }
                            else if (cmd == "設定報名期限") { data.DeadlineDay = day; data.DeadlineHour = h; data.DeadlineMinute = m; }
                            else { data.CancelDeadlineDay = day; data.CancelDeadlineHour = h; data.CancelDeadlineMinute = m; }
                            manager.Save(groupId, data); await lineClient.ReplyMessageAsync(replyToken, $"⚙️ {cmd}已更新");
                        }
                    }
                    continue;
                }

                if (cmd == "移除報名期限") { data.DeadlineDay = null; manager.Save(groupId, data); await lineClient.ReplyMessageAsync(replyToken, "✅ 已移除報名期限。"); continue; }
                if (cmd == "移除取消期限") { data.CancelDeadlineDay = null; manager.Save(groupId, data); await lineClient.ReplyMessageAsync(replyToken, "✅ 已移除取消期限。"); continue; }

                if (cmd == "增加季打" || cmd == "更新季打成員" || cmd == "移除季打")
                {
                    if (lines.Count >= 3)
                    {
                        var targetSet = (lines[1] == "男") ? data.MaleQuarterly : data.FemaleQuarterly;
                        if (cmd == "更新季打成員") targetSet.Clear();
                        foreach (var n in lines.Skip(2)) { if (cmd == "移除季打") targetSet.Remove(n); else targetSet.Add(n); }
                        manager.Save(groupId, data); _ = data.SyncToSheets(lineClient, groupId);
                        await lineClient.ReplyMessageAsync(replyToken, $"✅ {lines[1]}性季打名單已更新。");
                    }
                    continue;
                }

                if (cmd == "修改季打成員名稱" && lines.Count >= 3)
                {
                    string oldName = lines[1], newName = lines[2];
                    bool found = false;
                    if (data.MaleQuarterly.Contains(oldName)) { data.MaleQuarterly.Remove(oldName); data.MaleQuarterly.Add(newName); found = true; }
                    if (data.FemaleQuarterly.Contains(oldName)) { data.FemaleQuarterly.Remove(oldName); data.FemaleQuarterly.Add(newName); found = true; }
                    if (found)
                    {
                        var keys = data.WhiteList.Where(x => x.Value == oldName).Select(x => x.Key).ToList();
                        foreach (var k in keys) data.WhiteList[k] = newName;
                        manager.Save(groupId, data); _ = data.SyncToSheets(lineClient, groupId);
                        await lineClient.ReplyMessageAsync(replyToken, $"✅ {oldName} 已更名為 {newName}");
                    }
                    continue;
                }

                if (cmd == "查詢季打")
                {
                    await lineClient.ReplyMessageAsync(replyToken, $"📋 季打名單：\n男：{string.Join(", ", data.MaleQuarterly)}\n女：{string.Join(", ", data.FemaleQuarterly)}");
                    continue;
                }

                if (cmd == "增加報名" || cmd == "取消報名")
                {
                    string updateGuide = "⚠️ 指令格式已更新\n舊有的「增加/取消報名」指令已停用。\n現在請直接使用：\n➕ +1 男 姓名 (幫他人報名)\n➖ -1 男 姓名 (幫他人取消)\n或原有的 +1 男 / -1 男 進行個人報名。";
                    await lineClient.ReplyMessageAsync(replyToken, updateGuide);
                    continue;
                }
                if (cmd.StartsWith("設定冷氣模式"))
                {
                    data.IsAcAlwaysOn = userMessage.Contains("保持開啟");
                    manager.Save(groupId, data);
                    await lineClient.ReplyMessageAsync(replyToken, $"✅ 冷氣模式已設定為：{(data.IsAcAlwaysOn ? "保持開啟" : "保持關閉")}\n(此設定將套用於後續費用計算)");
                    continue;
                }
            }
            #endregion

            #region --- 一般使用者指令 ---
            // 偵測是否只輸入了 +1~18 但沒輸入性別
            var genderMissingMatch = Regex.Match(userMessage, @"^(\+|-)\s*(1[0-8]|[1-9])$");
            if (genderMissingMatch.Success)
            {
                await lineClient.ReplyMessageAsync(replyToken, "⚠️ 您好，您尚未輸入性別。\n範例：+1~18 男/女");
                continue;
            }

            // 使用 (1[0-8]|[1-9]) 來匹配 10-18 或 1-9
            var regMatch = Regex.Match(userMessage, @"^(\+|-)\s*(1[0-8]|[1-9])\s*(男|女)\s*(.*)$");
            if (regMatch.Success || userMessage == "查詢")
            {
                if (!data.WhiteList.TryGetValue(userId, out string? name) || name == null)
                {
                    await lineClient.ReplyMessageAsync(replyToken, "⚠️新朋友您好！ 請先輸入「申請綁定 您的暱稱」，再進行報名，感謝！");
                }
                else
                {
                    // --- 新的 Webhook 日期計算 ---
                    var now = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("Taipei Standard Time"));

                    // 統一調用 data 裡面的校準邏輯，確保與重置時間同步
                    var mDate = data.GetCalibratedMatchDate(now);

                    // 2. 🚩 [核心邏輯改動]：如果這個日期還沒到新賽季，自動「校準」到新賽季首戰
                    if (!string.IsNullOrEmpty(data.SeasonStart))
                    {
                        DateTime sStart = DateTime.ParseExact(data.SeasonStart, "yyyyMMdd", null);
                        
                        // 如果一般的下場比賽日還沒到開賽日，直接強制跳到開賽日之後的第一場
                        if (mDate < sStart.Date)
                        {
                            int diffToStart = ((int)data.MatchDay - (int)sStart.DayOfWeek + 7) % 7;
                            mDate = sStart.Date.AddDays(diffToStart);
                        }
                    }

                    // 3. 檢查是否已經超過賽季結束日 (這還是要擋，不然會報到明年去)
                    if (!string.IsNullOrEmpty(data.SeasonEnd))
                    {
                        DateTime sEnd = DateTime.ParseExact(data.SeasonEnd, "yyyyMMdd", null);
                        if (mDate > sEnd.Date)
                        {
                            await lineClient.ReplyMessageAsync(replyToken, "🚫 目前賽季已結束，請等待管理員更新下一季資訊。");
                            continue;
                        }
                    }

                    string dateKey = mDate.ToString("yyyyMMdd");
                    if (data.ClosedDates.ContainsKey(dateKey) && data.ClosedDates[dateKey])
                    {
                        await lineClient.ReplyMessageAsync(replyToken, $"您好，{mDate:yyyy/MM/dd}無開場，無需操作，感謝！");
                    }
                    else
                    {
                        if (userMessage == "查詢") { await lineClient.ReplyMessageAsync(replyToken, data.GetFormattedList("🏐 目前報名狀態")); }
                        else
                        {
                            string action = regMatch.Groups[1].Value;
                            int count = int.Parse(regMatch.Groups[2].Value);
                            string gender = regMatch.Groups[3].Value;
                            string targetName = regMatch.Groups[4].Value.Trim();
                            
                            // 決定報名姓名：若指令中有姓名則使用之，否則使用綁定姓名
                            string finalName = string.IsNullOrEmpty(targetName) ? name : targetName;

                            if (action == "+")
                            {
                                if (data.IsDeadlinePassed(data.DeadlineDay, data.DeadlineHour, data.DeadlineMinute))
                                    await lineClient.ReplyMessageAsync(replyToken, "⚠️ 已超過報名截止時間。");
                                else 
                                { 
                                    data.AddPlayer(finalName, count, gender); 
                                    manager.Save(groupId, data); 
                                    _ = data.SyncToSheets(lineClient, groupId); 
                                    
                                    string successMsg = string.IsNullOrEmpty(targetName) 
                                        ? $"✅ {finalName} 報名成功" 
                                        : $"✅ 已手動增加：{finalName} ({gender})";
                                    await lineClient.ReplyMessageAsync(replyToken, data.GetFormattedList(successMsg)); 
                                }
                            }
                            else
                            {
                                bool overdue = data.IsDeadlinePassed(data.CancelDeadlineDay, data.CancelDeadlineHour, data.CancelDeadlineMinute);
                                string res = data.RemovePlayer(finalName, count, overdue, gender);
                                
                                manager.Save(groupId, data); 
                                _ = data.SyncToSheets(lineClient, groupId); 
                                
                                string cancelMsg = string.IsNullOrEmpty(targetName)
                                    ? res
                                    : $"✅ 已手動取消：{finalName} ({gender})";
                                await lineClient.ReplyMessageAsync(replyToken, data.GetFormattedList(cancelMsg));
                            }
                        }
                    }
                }
                continue;
            }

            if (cmd.StartsWith("申請綁定"))
            {
                string targetName = cmd.Replace("申請綁定", "").Trim();
                if (!string.IsNullOrEmpty(targetName)) 
                {
                    bool isAlreadyBoundByOthers = data.WhiteList.Any(x => x.Key != userId && x.Value == targetName);
                    if (isAlreadyBoundByOthers) { await lineClient.ReplyMessageAsync(replyToken, "⚠️ 您好，此暱稱已被其他使用者綁定，請聯絡管理員或更換暱稱。"); }
                    else 
                    { 
                        data.WhiteList[userId] = targetName; 
                        manager.Save(groupId, data); 
                        bool isQuarterly = data.MaleQuarterly.Contains(targetName) || data.FemaleQuarterly.Contains(targetName);
                        string welcomeMsg = isQuarterly ? $"✅ 歡迎季打球員 {targetName}！身分已自動識別，可以開始報名囉！" : $"✅ 歡迎 {targetName} 綁定成功，可以開始報名囉！";
                        await lineClient.ReplyMessageAsync(replyToken, welcomeMsg); 
                    }
                }
                continue;
            }

            if (cmd == "幫助" || cmd == "指令") 
            { 
                string userHelp = @"┏━━ 🏐 AceLink 指令 ━━┓
    【 報名操作 】
    個人報名：
    ● 報名 ➜ +1~18 男/女
    ● 取消 ➜ -1~18 男/女

    幫他人報名
    ● 報名 ➜ +1~18 性別 姓名 
    ● 取消 ➜ -1~18 性別 姓名

    【 查詢與帳號 】
    ● 查詢 ➜ 顯示目前報名狀態
    ● 申請綁定 [暱稱]
┗━━━━━━━━━━━━┛";
                await lineClient.ReplyMessageAsync(replyToken, userHelp); 
                continue; 
            }
            #endregion
            manager.Save(groupId, data);
        }
    }
    catch (Exception ex) { Console.WriteLine(ex.Message); }
    return Results.Ok();
});

app.MapGet("/api/notes", (string groupId, VolleyManager manager) => { 
    // 1. 根據捷徑傳來的 groupId，去 GroupsData 資料夾找對應的 .json 檔案
    var data = manager.Load(groupId);
    
    // 2. 如果檔案不存在，回傳提示文字
    if (string.IsNullOrEmpty(data.GasUrl)) {
        return Results.Text("該群組尚未初始化資料", "text/plain", Encoding.UTF8);
    }

    // 3. 呼叫該群組資料物件裡面的格式化方法
    return Results.Text(data.GetIPhoneNoteFormat(), "text/plain", Encoding.UTF8); 
});
app.Run();
#endregion

#region --- 3. 資料模型與背景服務 ---
public class VolleyData
{
    public int MaxMale = 9; public int MaxFemale = 9;
    public List<string> MaleParticipants = new(); public List<string> FemaleParticipants = new();
    public List<string> MaleWaitingList = new(); public List<string> FemaleWaitingList = new();
    public HashSet<string> MaleQuarterly = new(); public HashSet<string> FemaleQuarterly = new();
    public Dictionary<string, string> WhiteList = new() { { "U4ae0a4b6b86b73455ca52ccab9ebc652", "Theo" } };
    public Dictionary<string, string> Admins = new(); // Key: UserID, Value: 暱稱
    public bool IsAuthorized = false; // 是否已授權此群組 (PlanA)
    public string GroupName { get; set; } = ""; // 群組暱稱
    public int QuarterlyFee = 0; public int AcFee = 0;
    public Dictionary<string, bool> AcRecords = new();
    public Dictionary<string, bool> ClosedDates = new();
    public DayOfWeek ResetDay = DayOfWeek.Saturday; public int ResetHour = 12; public int ResetMinute = 0;
    public DayOfWeek MatchDay = DayOfWeek.Saturday; public int MatchHour = 19; public int MatchMinute = 0;
    public DayOfWeek? DeadlineDay, CancelDeadlineDay;
    public int DeadlineHour, DeadlineMinute, CancelDeadlineHour, CancelDeadlineMinute;
    public string SeasonStart = ""; public string SeasonEnd = "";
    public int SetupStep = 0;
    public string GasUrl = "";
    public int PrepaidFee { get; set; } = 3000;
    public bool IsAcAlwaysOn = false;
    public bool ConfirmReset { get; set; } = false;
    public bool IsRecentlyReset { get; set; } = false; //標示本週是否已經完成過「手動換季重置」
    public bool IsResetEnabled { get; set; } = true; // 預設為開啟自動重置

    public DateTime GetCalibratedMatchDate(DateTime now) {
        // 取得本週五的日期基準
        int diffToFriday = ((int)MatchDay - (int)now.DayOfWeek);
        DateTime thisFriday = now.Date.AddDays(diffToFriday);

        // 取得本週六的重置時間點
        int diffToReset = ((int)ResetDay - (int)now.DayOfWeek);
        DateTime resetPoint = now.Date.AddDays(diffToReset).AddHours(ResetHour).AddMinutes(ResetMinute);

        // 核心判定：若現在時間還沒到重置點，目標日期鎖定在「本週五」
        // 若已過重置點，才跳到「下週五」
        DateTime targetDate = (now < resetPoint) ? thisFriday : thisFriday.AddDays(7);

        // 賽季首戰校準邏輯 (保留您原有的 SeasonStart 判斷)
        if (!string.IsNullOrEmpty(SeasonStart)) {
            DateTime sStart = DateTime.ParseExact(SeasonStart, "yyyyMMdd", null);
            if (targetDate < sStart.Date) {
                int diffToStart = ((int)MatchDay - (int)sStart.DayOfWeek + 7) % 7;
                targetDate = sStart.Date.AddDays(diffToStart);
            }
        }
        return targetDate;
    }    
    
    private DateTime GetNextMatchDate() {
        var now = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("Taipei Standard Time"));
        return GetCalibratedMatchDate(now);
    } 

    public string GetFormattedList(string title)
    {
        DateTime mDate = GetNextMatchDate();    
         
        var sb = new StringBuilder();
        sb.AppendLine($"📅 {mDate:yyyy/MM/dd} ({GetDayString(mDate.DayOfWeek)})");
        sb.AppendLine(title + "\n------------------");
        string[] board = new string[MaxMale + MaxFemale];
        HashSet<string> maleQUsed = new(); 
        HashSet<string> femaleQUsed = new();
        for (int i = 0; i < Math.Min(MaleParticipants.Count, MaxMale); i++) {
            string name = MaleParticipants[i];
            if (MaleQuarterly.Contains(name) && !maleQUsed.Contains(name)) { board[i] = name; maleQUsed.Add(name); }
            else board[i] = name + "(臨)";
        }
        for (int i = 0; i < Math.Min(FemaleParticipants.Count, MaxFemale); i++) {
            string name = FemaleParticipants[i];
            if (FemaleQuarterly.Contains(name) && !femaleQUsed.Contains(name)) { board[MaxMale + i] = name; femaleQUsed.Add(name); }
            else board[MaxMale + i] = name + "(臨)";
        }
        if (MaleParticipants.Count > MaxMale) {
            var extras = MaleParticipants.Skip(MaxMale).ToList();
            int slot = MaxMale;
            foreach (var p in extras) {
                while (slot < (MaxMale + MaxFemale) && !string.IsNullOrEmpty(board[slot])) slot++;
                if (slot < (MaxMale + MaxFemale)) {
                    bool isQ = MaleQuarterly.Contains(p) && !maleQUsed.Contains(p);
                    string identityTag = isQ ? "(男)" : "(臨)(男)";
                    if (isQ) maleQUsed.Add(p);
                    board[slot] = p + identityTag;
                }
            }
        }
        if (FemaleParticipants.Count > MaxFemale) {
            var extras = FemaleParticipants.Skip(MaxFemale).ToList();
            int slot = 0;
            foreach (var p in extras) {
                while (slot < MaxMale && !string.IsNullOrEmpty(board[slot])) slot++;
                if (slot < MaxMale) {
                    bool isQ = FemaleQuarterly.Contains(p) && !femaleQUsed.Contains(p);
                    string identityTag = isQ ? "(女)" : "(臨)(女)";
                    if (isQ) femaleQUsed.Add(p);
                    board[slot] = p + identityTag;
                }
            }
        }
        sb.AppendLine("男 =>");
        for (int i = 0; i < MaxMale; i++) sb.AppendLine($"{i + 1} : {board[i]}");
        sb.AppendLine("\n女 =>");
        for (int i = 0; i < MaxFemale; i++) sb.AppendLine($"{i + 1 + MaxMale} : {board[MaxMale + i]}");
        if (MaleWaitingList.Any() || FemaleWaitingList.Any()) {
            sb.AppendLine("\n--- 候補 ---");
            if (MaleWaitingList.Any()) sb.AppendLine($"男候補：{string.Join("，", MaleWaitingList.Select((p, i) => $"{i + 1}.{p}"))}");
            if (FemaleWaitingList.Any()) sb.AppendLine($"女候補：{string.Join("，", FemaleWaitingList.Select((p, i) => $"{i + 1}.{p}"))}");
        }
        return sb.ToString();
    }

    public void AddPlayer(string name, int count, string gender) {
        for (int i = 0; i < count; i++) {
            if (gender == "男") {
                if (MaleParticipants.Count + FemaleParticipants.Count < 18) MaleParticipants.Add(name);
                else MaleWaitingList.Add(name);
            } else {
                if (MaleParticipants.Count + FemaleParticipants.Count < 18) FemaleParticipants.Add(name);
                else FemaleWaitingList.Add(name);
            }
        }
        Rebalance();
    }

    private void Rebalance() {
        while (FemaleWaitingList.Any() && MaleParticipants.Count > 9) {
            string kickedName = MaleParticipants.Last();
            MaleParticipants.RemoveAt(MaleParticipants.Count - 1);
            MaleWaitingList.Insert(0, kickedName);
        }
        while (MaleWaitingList.Any() && FemaleParticipants.Count > 9) {
            string kickedName = FemaleParticipants.Last();
            FemaleParticipants.RemoveAt(FemaleParticipants.Count - 1);
            FemaleWaitingList.Insert(0, kickedName);
        }
        while (MaleParticipants.Count + FemaleParticipants.Count < 18 && (MaleWaitingList.Any() || FemaleWaitingList.Any())) {
            if (MaleParticipants.Count < 9 && MaleWaitingList.Any()) { MaleParticipants.Add(MaleWaitingList[0]); MaleWaitingList.RemoveAt(0); }
            else if (FemaleParticipants.Count < 9 && FemaleWaitingList.Any()) { FemaleParticipants.Add(FemaleWaitingList[0]); FemaleWaitingList.RemoveAt(0); }
            else if (MaleWaitingList.Any()) { MaleParticipants.Add(MaleWaitingList[0]); MaleWaitingList.RemoveAt(0); }
            else if (FemaleWaitingList.Any()) { FemaleParticipants.Add(FemaleWaitingList[0]); FemaleWaitingList.RemoveAt(0); }
            else break;
        }
    }

    public string RemovePlayer(string n, int c, bool o, string g) {
        List<string> list = (g == "男") ? MaleParticipants : FemaleParticipants;
        List<string> wait = (g == "男") ? MaleWaitingList : FemaleWaitingList;
        int rem = 0; bool warn = false;
        for (int i = 0; i < c; i++) {
            if (wait.Contains(n)) { wait.RemoveAt(wait.LastIndexOf(n)); rem++; }
            else if (list.Contains(n)) { list.RemoveAt(list.LastIndexOf(n)); rem++; if (o) warn = true; }
        }
        Rebalance();
        return warn ? $"{n}您好，因過取消期限若無遞補仍需繳費" : $"❌ {n} 已取消 {g} {rem}位";
    }

    public async Task SyncToSheets(ILineMessagingClient lineClient, string groupId, bool isNewSeason = false, string? targetDateKey = null) 
    {
        // 1. 安全檢查：確保有 GAS 網址
        if (string.IsNullOrEmpty(GasUrl)) {
            await lineClient.PushMessageAsync(groupId, "⚠️ 您好！您的機器人尚未做初始化設定，請輸入「系統初始化」讓我來引導你完成！\n(請先由管理員設定雲端網址)");
            return;
        }

        // 取得台灣當前時間
        var nowUtc = DateTime.UtcNow;
        var taiwanZone = TimeZoneInfo.FindSystemTimeZoneById("Taipei Standard Time");
        var now = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, taiwanZone);
        
        DateTime targetDate;

        // --- 核心日期判定邏輯 (賽季接力模式) ---

        if (!string.IsNullOrEmpty(targetDateKey) && targetDateKey.Length == 8) {
            targetDate = DateTime.ParseExact(targetDateKey, "yyyyMMdd", null);
        }
        else if (isNewSeason) {
            DateTime start = DateTime.ParseExact(SeasonStart, "yyyyMMdd", null);
            int diff = ((int)MatchDay - (int)start.DayOfWeek + 7) % 7;
            targetDate = start.AddDays(diff);
        }
        else {
            // 直接調用校準函式，這會參考您的重置時間 (週六 12:00)
            targetDate = GetCalibratedMatchDate(now);
        }

        // --- 準備傳送給 GAS 的資料 ---

        string dKey = targetDate.ToString("yyyyMMdd");
        
        // 判定名單：只有當目標日期是「最近的一個比賽日」時，才同步目前的報名清單
        // 若 targetDate 是遙遠的未來(指定日期)，則傳空清單，避免 GAS 誤填
        int currentMatchDiff = ((int)MatchDay - (int)now.DayOfWeek + 7) % 7;
        DateTime nextDefaultMatch = now.Date.AddDays(currentMatchDiff);
        if (currentMatchDiff == 0 && now.Hour >= MatchHour) nextDefaultMatch = nextDefaultMatch.AddDays(7);

        var finalParticipants = (targetDate.Date == nextDefaultMatch.Date) 
            ? MaleParticipants.Concat(FemaleParticipants).ToList() 
            : new List<string>();

        // 判定冷氣狀態：優先看手動紀錄，再看全域設定
        bool effectiveAcOn = AcRecords.ContainsKey(dKey) ? AcRecords[dKey] : IsAcAlwaysOn;

        var payload = new { 
            isNewSeason = isNewSeason, 
            seasonStart = SeasonStart, 
            seasonEnd = SeasonEnd, 
            matchDayStr = MatchDay.ToString(), 
            matchDate = targetDate.ToString("yyyy/MM/dd"), 
            currentParticipants = finalParticipants, 
            quarterlyMembers = MaleQuarterly.Concat(FemaleQuarterly).ToList(), 
            isAcOn = effectiveAcOn, 
            isClosed = ClosedDates.GetValueOrDefault(dKey, false), 
            quarterlyFee = QuarterlyFee, 
            acFee = AcFee,
            prepaidFee = this.PrepaidFee, 
            isFuture = targetDate.Date > nextDefaultMatch.Date,
            isAcAlwaysOn = IsAcAlwaysOn, 
            headerOrder = new[] { "姓名", "提前收費金額", "應收總額", "退費", "請假次數" }
        };

        // --- 執行網路傳輸 ---
        using var client = new HttpClient();
        try { 
            var json = JsonConvert.SerializeObject(payload);
            await client.PostAsync(GasUrl, new StringContent(json, Encoding.UTF8, "application/json")); 
        } 
        catch (Exception ex) {
            Console.WriteLine($"GAS Sync Error: {ex.Message}");
        }
    }

    public void ResetToQuarterly() {
        MaleParticipants.Clear(); FemaleParticipants.Clear(); 
        MaleWaitingList.Clear(); FemaleWaitingList.Clear();
        // 確保只加入現有的季打
        MaleParticipants.AddRange(MaleQuarterly.Take(MaxMale)); 
        FemaleParticipants.AddRange(FemaleQuarterly.Take(MaxFemale));
        
        ClosedDates.Clear();
        AcRecords.Clear(); // 🚩 必加：避免舊的冷氣紀錄帶到新的一週
    }
    private string GetDayString(DayOfWeek d) => d switch { DayOfWeek.Monday=>"一", DayOfWeek.Tuesday=>"二", DayOfWeek.Wednesday=>"三", DayOfWeek.Thursday=>"四", DayOfWeek.Friday=>"五", DayOfWeek.Saturday=>"六", DayOfWeek.Sunday=>"日", _=>"" };
    public bool IsDeadlinePassed(DayOfWeek? targetDay, int h, int m)
    {
        if (!targetDay.HasValue) return false;
        var nowUtc = DateTime.UtcNow;
        var taiwanZone = TimeZoneInfo.FindSystemTimeZoneById("Taipei Standard Time");
        var now = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, taiwanZone);
        int diffToMatch = ((int)MatchDay - (int)now.DayOfWeek + 7) % 7;
        if (diffToMatch == 0 && (now.Hour > MatchHour || (now.Hour == MatchHour && now.Minute >= MatchMinute))) diffToMatch = 7;
        DateTime nextMatchDate = now.Date.AddDays(diffToMatch);
        int diffToDeadline = ((int)nextMatchDate.DayOfWeek - (int)targetDay.Value + 7) % 7;
        DateTime deadlineDate = nextMatchDate.AddDays(-diffToDeadline);
        DateTime finalDeadline = deadlineDate.AddHours(h).AddMinutes(m);
        return now > finalDeadline;
    }

    public string GetIPhoneNoteFormat()
    {
        DateTime targetDate = GetNextMatchDate();

        // 4. 開始組合字串
        var sb = new StringBuilder();
        sb.AppendLine($"🏐 {targetDate:MM/dd} 臨打收款清單");
        sb.AppendLine("------------------");

        // 5. 抓取名單並過濾出「臨打」人員
        // 呼叫原本的 GetFormattedList 取得包含 (臨) 標記的字串
        string fullList = this.GetFormattedList("iPhone產製");
        
        var lines = fullList.Split('\n');
        var tempPlayers = new List<string>();

        foreach (var line in lines)
        {
            // 只要該行包含 "(臨)"，就代表是需要收費的臨打人員
            if (line.Contains("(臨)"))
            {
                // 擷取冒號後面的名字，例如 "1 : 小明(臨)" -> "小明"
                var parts = line.Split(':');
                if (parts.Length > 1)
                {
                    string name = parts[1].Replace("(臨)", "").Replace("(男)", "").Replace("(女)", "").Trim();
                    if (!string.IsNullOrEmpty(name))
                    {
                        tempPlayers.Add(name);
                    }
                }
            }
        }

        // 6. 輸出結果
        if (tempPlayers.Count == 0)
        {
            sb.AppendLine("(目前無臨打人員)");
        }
        else
        {
            foreach (var p in tempPlayers)
            {
                sb.AppendLine(p);
            }
        }

        return sb.ToString().Trim();
    }
}

public class ResetTaskService : BackgroundService {
    private readonly VolleyManager _manager;
    private readonly ILineMessagingClient _lineClient;
    public ResetTaskService(VolleyManager manager, ILineMessagingClient lineClient) { _manager = manager; _lineClient = lineClient; }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        while (!stoppingToken.IsCancellationRequested) {
            var nowUtc = DateTime.UtcNow;
            var taiwanZone = TimeZoneInfo.FindSystemTimeZoneById("Taipei Standard Time");
            var now = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, taiwanZone);
            
            if (Directory.Exists("GroupsData")) {
                var files = Directory.GetFiles("GroupsData", "*.json");
                var tasks = new List<Task>();

                foreach (var file in files) {
                    string gId = Path.GetFileNameWithoutExtension(file);
                    
                    // 為了避免大規模併發讀寫衝突，將每個群組處理邏輯封裝
                    tasks.Add(Task.Run(async () => {
                        try {
                            var data = _manager.Load(gId);
                            
                            // 判定是否達到重置時間 (且這分鐘內還沒處理過)
                            if (now.DayOfWeek == data.ResetDay && now.Hour == data.ResetHour && now.Minute == data.ResetMinute) 
                            {
                                // 1. 檢查開關
                                if (!data.IsResetEnabled) return; 

                                // 2. 處理「本週已手動重置過」的狀態
                                if (data.IsRecentlyReset) 
                                {
                                    data.IsRecentlyReset = false; 
                                    _manager.Save(gId, data);
                                    return; 
                                }

                                // 3. 執行重置與存檔
                                data.ResetToQuarterly(); 
                                _manager.Save(gId, data); 

                                // 4. 同步至雲端 (多群組併發呼叫)
                                await data.SyncToSheets(_lineClient, gId);

                                // 5. 取得名單內容 (選用推播)
                                // string listContent = data.GetFormattedList("🏐 本週預設名單");
                                // await _lineClient.PushMessageAsync(gId, $"🧹 【AceLink 自動重置完成】\n{listContent}");
                            }
                        }
                        catch (Exception ex) {
                            string errorInfo = $"❌ 重置異常報告\n群組: {gId}\n原因: {ex.Message}";
                            try {
                                await _lineClient.PushMessageAsync("U4ae0a4b6b86b73455ca52ccab9ebc652", errorInfo);
                            } catch {
                                Console.WriteLine(errorInfo);
                            }
                        }
                    }, stoppingToken));
                }

                // 同時啟動所有群組的檢查與處理，不互相阻塞
                if (tasks.Any()) await Task.WhenAll(tasks);
            }

            // 縮短檢查頻率至 30 秒，配合精確的分鐘判定，確保每一分鐘都會被掃描到至少一次
            await Task.Delay(30000, stoppingToken);
        }
    }
}

public class VolleyManager {
    public Dictionary<string, string> PendingImports { get; } = new();
    public Dictionary<string, string> PendingDeletes { get; } = new();
    private readonly string _path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "GroupsData");
    public VolleyManager() { if (!Directory.Exists(_path)) Directory.CreateDirectory(_path); }
    public VolleyData Load(string id) {
        string f = Path.Combine(_path, $"{id}.json");
        return File.Exists(f) ? JsonConvert.DeserializeObject<VolleyData>(File.ReadAllText(f)) ?? new VolleyData() : new VolleyData();
    }
    public void Save(string id, VolleyData d) => File.WriteAllText(Path.Combine(_path, $"{id}.json"), JsonConvert.SerializeObject(d, Formatting.Indented));
}


#endregion