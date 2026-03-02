# 使用 .NET 9 SDK 進行編譯
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /app

# 複製專案檔並還原套件
COPY *.csproj ./
RUN dotnet restore

# 複製所有程式碼並發佈
COPY . ./
RUN dotnet publish -c Release -o out

# 使用 .NET 9 runtime 環境執行
FROM mcr.microsoft.com/dotnet/aspnet:9.0
WORKDIR /app
COPY --from=build /app/out .

# 執行專案
ENTRYPOINT ["dotnet", "VolleyManager.dll"]
