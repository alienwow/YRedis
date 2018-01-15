$ErrorActionPreference = 'Stop'
# Environment helpers ------------------------------------
Function Get-MsBuildPath() {
    $msBuildRegPath = "HKLM:\SOFTWARE\Microsoft\MSBuild\ToolsVersions\14.0"
    $msBuildPathRegItem = Get-ItemProperty $msBuildRegPath -Name "MSBuildToolsPath"
    # $msBuildPath = $msBuildPathRegItem.MsBuildToolsPath + "msbuild.exe"
    $msBuildPath = "C:\Program Files (x86)\Microsoft Visual Studio\2017\Enterprise\MSBuild\15.0\Bin\msbuild.exe"
    return $msBuildPath
}
# Environment variables ----------------------------------
# $global_buildDirPath = Get-Location
$global_buildDirPath = ".\"
$global_msBuildPath = Get-MsBuildPath
$global_solutionPath = "$global_buildDirPath"
$global_solutionFilePath = "$global_solutionPath\YRedis.sln"
$global_nugetPath = "$global_buildDirPath\build\tools\.nuget\nuget.exe"
# Install nuget packages ---------------------------------
Function Install-SolutionPackages() {
    iex "$global_nugetPath restore $global_solutionFilePath"
}
$project_path = $global_solutionPath + '\YRedis\YRedis.csproj'
Function Compile-Project() {
    iex -Command "& '$global_msBuildPath' '$global_solutionFilePath' /t:Clean /p:Configuration=Release /p:Platform=""Any CPU"""
}
Install-SolutionPackages
Compile-Project