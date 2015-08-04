$root = (split-path -parent $MyInvocation.MyCommand.Definition) + '\..'
$version = [System.Reflection.Assembly]::LoadFile("{0}\src\Migrator\bin\Migrator\Debug\Migrator.dll" -f ($root)).GetName().Version
$versionStr = "{0}.{1}.{2}" -f ($version.Major, $version.Minor, $version.Build)

Write-Host "Setting .nuspec version tag to $versionStr"

$content = (Get-Content $root\NuGet\Package.nuspec) 
$content = $content -replace '\$version\$',$versionStr

$content | Out-File $root\nuget\Package.compiled.nuspec

& $root\NuGet\NuGet.exe pack $root\nuget\Package.compiled.nuspec