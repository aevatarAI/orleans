#!/bin/bash
# 构建 Orleans.Core metrics 版本包的脚本

set -e

echo "Building Orleans.Core 9.0.2-metrics-v2 package..."

# 清理之前的构建
echo "Cleaning previous builds..."
dotnet clean src/Orleans.Core/Orleans.Core.csproj -c Release

# 构建包（使用 PackageReference 而不是 ProjectReference）
echo "Building package with flexible dependency..."
dotnet pack src/Orleans.Core/Orleans.Core.csproj -c Release -p:UseProjectReferences=false -p:PackageVersion=9.0.2-metrics-v2

# 显示生成的包
echo "Generated package:"
ls -la Artifacts/Release/Microsoft.Orleans.Core.*.nupkg

echo "Package built successfully!"
echo ""
echo "To publish to MyGet:"
echo "dotnet nuget push Artifacts/Release/Microsoft.Orleans.Core.9.0.2-metrics-v2.nupkg -s https://www.myget.org/F/aelf-project-dev/api/v2/package -k YOUR_API_KEY"