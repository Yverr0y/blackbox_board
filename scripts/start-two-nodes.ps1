param(
  [string]$PortA = "7861",
  [string]$PortB = "7862",
  [string]$MeshPortA = "COM5",
  [string]$MeshPortB = "COM3"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot

$nodeACommand = @"
Set-Location '$root'
`$env:PORT='$PortA'
`$env:MESHTASTIC_PORT='$MeshPortA'
`$env:DATA_DIR='$root\data-node-a'
`$env:INSTANCE_LABEL='node-a'
node .\server.js
"@

$nodeBCommand = @"
Set-Location '$root'
`$env:PORT='$PortB'
`$env:MESHTASTIC_PORT='$MeshPortB'
`$env:DATA_DIR='$root\data-node-b'
`$env:INSTANCE_LABEL='node-b'
node .\server.js
"@

Start-Process powershell -ArgumentList "-NoExit", "-Command", $nodeACommand | Out-Null
Start-Sleep -Milliseconds 500
Start-Process powershell -ArgumentList "-NoExit", "-Command", $nodeBCommand | Out-Null
