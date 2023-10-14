Connect-MgGraph

$graphAppId = "00000003-0000-0000-c000-000000000000"

$graphPrincipal = Get-AzADServicePrincipal -Filter "appId eq '$graphAppId'"

$systemIdentity = Get-AzADServicePrincipal -Filter "displayName eq 'cantori-attendance'"

$role = $graphPrincipal.AppRole | Where-Object { $_.Value -eq "Mail.Send" }

New-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $systemIdentity.Id -PrincipalId $systemIdentity.Id -ResourceId $graphPrincipal.Id -AppRoleId $role.Id