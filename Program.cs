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
            var adminList = new HashSet<string> { 
                "U4ae0a4b6b86b73455ca52ccab9ebc652", // Theo
                "U5b9e1a119b4fbdf57efadd12644e33ed"  // 阿平
            };            
            bool isAdmin = adminList.Contains(userId);
            if (string.IsNullOrEmpty(userMessage)) continue;

            if (userMessage == "我的ID")
            {
                string status = data.WhiteList.ContainsKey(userId) ? $"已綁定：{data.WhiteList[userId]}" : "尚未綁定";
                await lineClient.ReplyMessageAsync(replyToken, $"您的 LINE ID：\n{userId}\n\n目前狀態：{status}");
                continue;
            }

            var lines = userMessage.Split('\n').Select(l => l.Trim()).ToList();
            string cmd = lines[0];

            #region --- 管理員指令區 ---
            if (isAdmin)
            {
                if (cmd.StartsWith("設定雲端網址"))
                {
                    string url = userMessage.Replace("設定雲端網址", "").Trim();
                    if (Uri.IsWellFormedUriString(url, UriKind.Absolute))
                    {
                        data.GasUrl = url;
                        manager.Save(groupId, data);
                        await lineClient.ReplyMessageAsync(replyToken, "✅ 雲端 GAS 網址設定成功！\n現在可以進行初始化或同步了。");
                    }
                    else { await lineClient.ReplyMessageAsync(replyToken, "❌ 網址格式錯誤，請輸入完整的 https://... 網址"); }
                    continue;
                }

                if (cmd == "重置")
                {
                    data.ConfirmReset = true;
                    await lineClient.ReplyMessageAsync(replyToken, "⚠️ 【安全確認】您確定要重置嗎？\n這將恢復季打並清空候補。\n\n請在 30 秒內回覆「確認重置」\n或輸入「取消」以終止。");
                    continue;
                }
                if (data.ConfirmReset)
                {
                    if (userMessage.Contains("取消"))
                    {
                        data.ConfirmReset = false;
                        await lineClient.ReplyMessageAsync(replyToken, "❌ 已取消重置，資料未變動。");
                    }
                    else if (cmd == "確認重置")
                    {
                        data.ConfirmReset = false;
                        data.ResetToQuarterly(); manager.Save(groupId, data); _ = data.SyncToSheets(lineClient, groupId);
                        await lineClient.ReplyMessageAsync(replyToken, "🧹 已完成重置：恢復季打名單並清空候補。");
                    }
                    continue;
                }

                if (cmd == "系統初始化")
                {
                    data.SetupStep = 1;
                    manager.Save(groupId, data);
                    await lineClient.ReplyMessageAsync(replyToken, "🛠️ 【AceLink 系統初始化】已啟動\n\n[Step 1/5] 設定球季期間\n請輸入起訖日期，格式如下：\n20260101\n20260331\n(或輸入「取消設定」退出)");
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
                            await lineClient.ReplyMessageAsync(replyToken, "✅ 球季期間已設定。\n\n[Step 2/5] 設定比賽與費用\n請輸入格式：\n星期 (英文)\n時間 (HHmm)\n季打費用\n冷氣費用\n\n範例：\nSaturday\n1900\n200\n40");
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
                                await lineClient.ReplyMessageAsync(replyToken, "✅ 費用與時間已設定。\n\n[Step 3/5] 匯入季打名單\n請一次性輸入性別與名單，格式如下：\n男\n小明,小李,小張\n女\n小美,小華");
                            }
                        } else { await lineClient.ReplyMessageAsync(replyToken, "⚠️ 格式錯誤，請檢查星期與費用格式。"); }
                        continue;
                    }
                    if (data.SetupStep == 3)
                    {
                        int maleIdx = lines.IndexOf("男");
                        int femaleIdx = lines.IndexOf("女");
                        if (maleIdx != -1 && femaleIdx != -1) {
                            data.MaleQuarterly.Clear(); data.FemaleQuarterly.Clear();
                            string mNames = lines[maleIdx + 1];
                            string fNames = lines[femaleIdx + 1];
                            foreach(var n in mNames.Split(new[] { ',', '，' })) { if(!string.IsNullOrWhiteSpace(n)) data.MaleQuarterly.Add(n.Trim()); }
                            foreach(var n in fNames.Split(new[] { ',', '，' })) { if(!string.IsNullOrWhiteSpace(n)) data.FemaleQuarterly.Add(n.Trim()); }
                            data.SetupStep = 4; manager.Save(groupId, data);
                            await lineClient.ReplyMessageAsync(replyToken, "✅ 季打名單已匯入。\n\n[Step 4/5] 設定重置與取消期限\n請輸入格式：\n重置星期/時間\n取消截止星期/時間\n\n範例：\nSaturday/1200\nThursday/1500");
                        } else { await lineClient.ReplyMessageAsync(replyToken, "⚠️ 格式錯誤，請確保包含「男」與「女」標籤及名單。"); }
                        continue;
                    }
                    if (data.SetupStep == 4)
                    {
                        if (lines.Count >= 2) {
                            var p1 = lines[0].Split('/'); var p2 = lines[1].Split('/');
                            if (Enum.TryParse<DayOfWeek>(p1[0], true, out var rDay) && Enum.TryParse<DayOfWeek>(p2[0], true, out var cDay)) {
                                data.ResetDay = rDay; data.ResetHour = int.Parse(p1[1].Substring(0, 2)); data.ResetMinute = int.Parse(p1[1].Substring(2));
                                data.CancelDeadlineDay = cDay; data.CancelDeadlineHour = int.Parse(p2[1].Substring(0, 2)); data.CancelDeadlineMinute = int.Parse(p2[1].Substring(2));
                                data.SetupStep = 5; manager.Save(groupId, data);
                                await lineClient.ReplyMessageAsync(replyToken, "✅ 自動化邏輯已設定。\n\n[Step 5/5] 確認並同步\n系統將進行重置並同步至 Google Sheets。\n輸入「確認完成」以結束設定。");
                            }
                        } else { await lineClient.ReplyMessageAsync(replyToken, "⚠️ 格式錯誤，請檢查日期與時間格式。"); }
                        continue;
                    }
                    if (data.SetupStep == 5)
                    {
                        if (cmd == "確認完成") {
                            await lineClient.ReplyMessageAsync(replyToken, "⏳ 正在生成初始雲端表格與重置名單，請稍候...");
                            _ = Task.Run(async () => {
                                try {
                                    data.SetupStep = 0; 
                                    data.ResetToQuarterly(); 
                                    manager.Save(groupId, data); 
                                    await data.SyncToSheets(lineClient, groupId, true);
                                    await lineClient.PushMessageAsync(groupId, "🎊 系統初始化完成！\n季打名單已恢復，且雲端表格已同步生成。");
                                } catch {
                                    await lineClient.PushMessageAsync(groupId, "❌ 系統初始化成功但雲端同步失敗，請檢查 GAS 設定。");
                                }
                            });
                        }
                        continue;
                    }
                }

                if (cmd == "管理員指令")
                {
                    string helpMsg = @"┏━🏐 AceLink 管理員中心━┓
【 核心配置 】
● 系統初始化 ➜ 啟動引導式流程
● 重置 ➜ 恢復當週季打名單並清空候補

【 賽季與期限設定 】
● 設定季打時間並創建新表格
開始日 ↵ 結束日 ↵ 星期 ↵ 時間

● 設定[報名/取消/重置]期限
● 移除[報名/取消]期限

【 費用與日期控制 】
● 設定[季打/冷氣]費用 [金額]
● [8位日期] [無開場/有開場]
● [8位日期] [開冷氣/關冷氣]

【 成員維護 】
● 查詢季打 / 增加季打 / 移除季打
● 修改季打成員名稱 ↵ 舊名 ↵ 新名

【 手動報名干預 】
● 增加報名 / 取消報名 ↵ 性別 ↵ 姓名
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

                if (cmd == "設定季打時間" && lines.Count >= 5)
                {
                    data.SeasonStart = lines[1]; data.SeasonEnd = lines[2];
                    if (Enum.TryParse<DayOfWeek>(lines[3], true, out var day))
                    {
                        data.MatchDay = day;
                        string timeStr = lines[4].Replace(":", "").Trim();
                        if (timeStr.Length >= 3 && int.TryParse(timeStr.Substring(0, timeStr.Length - 2), out int h) && int.TryParse(timeStr.Substring(timeStr.Length - 2), out int m))
                        {
                            data.MatchHour = h; data.MatchMinute = m;
                            manager.Save(groupId, data); 
                            await lineClient.ReplyMessageAsync(replyToken, "⏳ 正在生成新賽季表格，這可能需要幾秒鐘，請稍候...");
                            _ = Task.Run(async () => {
                                try {
                                    await data.SyncToSheets(lineClient, groupId, true);
                                    await lineClient.PushMessageAsync(groupId, $"✅ 季度設定成功！\n期間：{data.SeasonStart}~{data.SeasonEnd}\n雲端表格已同步生成。");
                                } catch {
                                    await lineClient.PushMessageAsync(groupId, "❌ 雲端同步失敗，請檢查網絡或 GAS 設定。");
                                }
                            });
                        }
                    }
                    continue;
                }

                if (cmd == "設定重置" || cmd == "設定報名期限" || cmd == "設定取消期限")
                {
                    if (lines.Count >= 3 && Enum.TryParse<DayOfWeek>(lines[1], true, out var day))
                    {
                        string timeStr = lines[2].Replace(":", "");
                        if (timeStr.Length >= 3 && int.TryParse(timeStr.Substring(0, timeStr.Length - 2), out int h) && int.TryParse(timeStr.Substring(timeStr.Length - 2), out int m))
                        {
                            if (cmd == "設定重置") { data.ResetDay = day; data.ResetHour = h; data.ResetMinute = m; }
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

                if ((cmd == "增加報名" || cmd == "取消報名") && lines.Count >= 3)
                {
                    string gender = lines[1];
                    string pName = lines[2];
                    if (cmd == "增加報名") { data.AddPlayer(pName, 1, gender); manager.Save(groupId, data); _ = data.SyncToSheets(lineClient, groupId); await lineClient.ReplyMessageAsync(replyToken, data.GetFormattedList($"✅ 已手動增加：{pName} ({gender})")); }
                    else { string resMsg = data.RemovePlayer(pName, 1, false, gender); manager.Save(groupId, data); _ = data.SyncToSheets(lineClient, groupId); await lineClient.ReplyMessageAsync(replyToken, data.GetFormattedList($"✅ 已手動取消：{pName} ({gender})")); }
                    continue;
                }
            }
            #endregion

            #region --- 一般使用者指令 ---
            var genderMissingMatch = Regex.Match(userMessage, @"^(\+|-)\s*([1-2])$");
            if (genderMissingMatch.Success)
            {
                await lineClient.ReplyMessageAsync(replyToken, "⚠️ 您好，您尚未輸入性別。\n範例：+1男 或 -1女");
                continue;
            }

            var regMatch = Regex.Match(userMessage, @"^(\+|-)\s*([1-2])\s*(男|女)$");
            if (regMatch.Success || userMessage == "查詢")
            {
                if (!data.WhiteList.TryGetValue(userId, out string? name) || name == null)
                {
                    await lineClient.ReplyMessageAsync(replyToken, "⚠️新朋友您好！ 請先輸入「申請綁定 您的暱稱」，再進行報名，感謝！");
                }
                else
                {
                    var mDate = DateTime.Now.Date.AddDays(((int)data.MatchDay - (int)DateTime.Now.DayOfWeek + 7) % 7);
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
                            if (action == "+")
                            {
                                if (data.IsDeadlinePassed(data.DeadlineDay, data.DeadlineHour, data.DeadlineMinute))
                                    await lineClient.ReplyMessageAsync(replyToken, "⚠️ 已超過報名截止時間。");
                                else { data.AddPlayer(name, count, gender); manager.Save(groupId, data); _ = data.SyncToSheets(lineClient, groupId); await lineClient.ReplyMessageAsync(replyToken, data.GetFormattedList($"✅ {name} 報名成功")); }
                            }
                            else
                            {
                                bool overdue = data.IsDeadlinePassed(data.CancelDeadlineDay, data.CancelDeadlineHour, data.CancelDeadlineMinute);
                                string res = data.RemovePlayer(name, count, overdue, gender);
                                manager.Save(groupId, data); _ = data.SyncToSheets(lineClient, groupId); await lineClient.ReplyMessageAsync(replyToken, data.GetFormattedList(res));
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
● 報名 ➜ +1男 / +2女
● 取消 ➜ -1男 / -1女

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
    var data = manager.Load(groupId);
    return Results.Text(data.GetIPhoneNoteFormat()); 
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
    [JsonIgnore] public bool ConfirmReset = false;

    public void Save() { } 
    public static VolleyData Load() => new VolleyData();

    public string GetFormattedList(string title)
    {
        int diff = ((int)MatchDay - (int)DateTime.Now.DayOfWeek + 7) % 7;
        var mDate = DateTime.Now.Date.AddDays(diff);
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

    public async Task SyncToSheets(ILineMessagingClient lineClient, string groupId, bool isNewSeason = false, string? targetDateKey = null) {
        if (string.IsNullOrEmpty(GasUrl)) {
            await lineClient.PushMessageAsync(groupId, "⚠️ 您好！您的機器人尚未做初始化設定，請輸入「系統初始化」讓我來引導你完成！\n(請先由管理員設定雲端網址)");
            return;
        }
        int nextMatchDiff = ((int)MatchDay - (int)DateTime.Now.DayOfWeek + 7) % 7;
        DateTime nextMatchDate = DateTime.Now.Date.AddDays(nextMatchDiff);
        DateTime targetDate = (!string.IsNullOrEmpty(targetDateKey) && targetDateKey.Length == 8) 
            ? new DateTime(int.Parse(targetDateKey.Substring(0,4)), int.Parse(targetDateKey.Substring(4,2)), int.Parse(targetDateKey.Substring(6,2))) : nextMatchDate;
        var finalParticipants = (targetDate.Date == nextMatchDate.Date) ? MaleParticipants.Concat(FemaleParticipants).ToList() : new List<string>();
        string dKey = targetDate.ToString("yyyyMMdd");
        // 計算是否為未來場次 (大於下一次比賽日)
        bool isFuture = targetDate.Date > nextMatchDate.Date;

        var payload = new { 
            isNewSeason = isNewSeason, 
            seasonStart = SeasonStart, 
            seasonEnd = SeasonEnd, 
            matchDayStr = MatchDay.ToString(), 
            matchDate = targetDate.ToString("yyyy/MM/dd"), 
            currentParticipants = finalParticipants, 
            quarterlyMembers = MaleQuarterly.Concat(FemaleQuarterly).ToList(), 
            isAcOn = AcRecords.GetValueOrDefault(dKey, false), 
            isClosed = ClosedDates.GetValueOrDefault(dKey, false), 
            quarterlyFee = QuarterlyFee, 
            acFee = AcFee,
            isFuture = isFuture // 新增此旗標
        };    
        using var client = new HttpClient();
        try { await client.PostAsync(GasUrl, new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json")); } catch { }
    }

    public void ResetToQuarterly() {
        MaleParticipants.Clear(); FemaleParticipants.Clear(); MaleWaitingList.Clear(); FemaleWaitingList.Clear();
        MaleParticipants.AddRange(MaleQuarterly.Take(MaxMale)); FemaleParticipants.AddRange(FemaleQuarterly.Take(MaxFemale));
        ClosedDates.Clear();
    }
    private string GetDayString(DayOfWeek d) => d switch { DayOfWeek.Monday=>"一", DayOfWeek.Tuesday=>"二", DayOfWeek.Wednesday=>"三", DayOfWeek.Thursday=>"四", DayOfWeek.Friday=>"五", DayOfWeek.Saturday=>"六", DayOfWeek.Sunday=>"日", _=>"" };
    public bool IsDeadlinePassed(DayOfWeek? targetDay, int h, int m)
    {
        if (!targetDay.HasValue) return false;
        var now = DateTime.Now;
        int diffToMatch = ((int)MatchDay - (int)now.DayOfWeek + 7) % 7;
        if (diffToMatch == 0 && (now.Hour > MatchHour || (now.Hour == MatchHour && now.Minute >= MatchMinute))) diffToMatch = 7;
        DateTime nextMatchDate = now.Date.AddDays(diffToMatch);
        int diffToDeadline = ((int)nextMatchDate.DayOfWeek - (int)targetDay.Value + 7) % 7;
        DateTime deadlineDate = nextMatchDate.AddDays(-diffToDeadline);
        DateTime finalDeadline = deadlineDate.AddHours(h).AddMinutes(m);
        return now > finalDeadline;
    }
    public string GetIPhoneNoteFormat() { 
        DateTime now = DateTime.Now; int diffToFriday = (int)DayOfWeek.Friday - (int)now.DayOfWeek;
        DateTime targetFriday = now.AddDays(diffToFriday).Date;
        int diffToReset = (int)ResetDay - (int)now.DayOfWeek;
        DateTime resetDeadline = now.AddDays(diffToReset).Date.AddHours(ResetHour).AddMinutes(ResetMinute);
        if (now >= resetDeadline) targetFriday = targetFriday.AddDays(7);
        var sb = new StringBuilder(); sb.AppendLine($"🏐 {targetFriday:MM/dd} 臨打收款清單"); 
        string fullList = GetFormattedList("iPhone"); 
        var tempPlayers = fullList.Split('\n').Where(line => line.Contains("(臨)")).Select(line => line.Split(':').Last().Trim()).ToList();
        foreach (var p in tempPlayers) sb.AppendLine($"{p}"); return sb.ToString().Trim();
    }
}

public class ResetTaskService : BackgroundService {
    private readonly VolleyManager _manager;
    private readonly ILineMessagingClient _lineClient;
    public ResetTaskService(VolleyManager manager, ILineMessagingClient lineClient) { _manager = manager; _lineClient = lineClient; }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        while (!stoppingToken.IsCancellationRequested) {
            var now = DateTime.Now;
            if (Directory.Exists("GroupsData")) {
                var files = Directory.GetFiles("GroupsData", "*.json");
                foreach (var file in files) {
                    string gId = Path.GetFileNameWithoutExtension(file);
                    var data = _manager.Load(gId);
                    if (now.DayOfWeek == data.ResetDay && now.Hour == data.ResetHour && now.Minute == data.ResetMinute) {
                        data.ResetToQuarterly(); _manager.Save(gId, data); _ = data.SyncToSheets(_lineClient, gId);
                    }
                }
            }
            await Task.Delay(60000, stoppingToken);
        }
    }
}

public class VolleyManager {
    private readonly string _path = "GroupsData";
    public VolleyManager() { if (!Directory.Exists(_path)) Directory.CreateDirectory(_path); }
    public VolleyData Load(string id) {
        string f = Path.Combine(_path, $"{id}.json");
        return File.Exists(f) ? JsonConvert.DeserializeObject<VolleyData>(File.ReadAllText(f)) ?? new VolleyData() : new VolleyData();
    }
    public void Save(string id, VolleyData d) => File.WriteAllText(Path.Combine(_path, $"{id}.json"), JsonConvert.SerializeObject(d, Formatting.Indented));
}
#endregion