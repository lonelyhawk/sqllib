function DeployDacpac {
    param(
        [Parameter(Mandatory=$true)]
        [string]$dacpacPath,
        [Parameter(Mandatory=$true)]
        [string]$databaseName,
        [Parameter(Mandatory=$true)]
        [string]$outputPath,
        [Parameter(Mandatory=$false)]
        [string]$server="localhost",
        [Parameter(Mandatory=$false)]
        [string]$appName=$databaseName,
        [Parameter(Mandatory=$false)]
        [ValidateSet("DeployReport","DriftReport","Publish","Script")]
        [Alias('Action')]
        [string]$pkgAction='Script',
        [Parameter(Mandatory=$false)]
        [boolean]$attachAsArtefacts=$false,
        [Parameter(Mandatory=$false)]
        [string]$additionalParams='',
        [Parameter(Mandatory=$false)]
        [AllowEmptyCollection()]
        [hashtable]$sqlCmdVars=@{},
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether CLR deployment will cause blocking assemblies to be dropped')]
        [boolean]$AllowDropBlockingAssemblies=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether deployment will block due to platform compatibility.')]
        [boolean]$AllowIncompatiblePlatform=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether a database backup will be performed before proceeding with the actual deployment actions.')]
        [boolean]$BackupDatabaseBeforeChanges=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether deployment should stop if the operation could cause data loss.')]
        [boolean]$BlockOnPossibleDataLoss=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether the system will check for differences between the present state of the database and the registered state of the database '+
        'and block deployment if changes are detected. Even if this option is set to true, drift detection will only occur on a database if it was previously deployed with the '+
        'RegisterDataTierApplication option enabled.')]
        [boolean]$BlockWhenDriftDetected=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether the declaration of SQLCMD variables are commented out in the script header.')]
        [boolean]$CommentOutSetVarDeclarations=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether the source collation will be used for identifier comparison.')]
        [boolean]$CompareUsingTargetCollation=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether the existing database will be dropped and a new database created before proceeding with the actual deployment actions.'+ 
        'Acquires single-user mode before dropping the existing database.')]
        [boolean]$CreateNewDatabase=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether the system will acquire single-user mode on the target database during the duration of the deployment operation.')]
        [boolean]$DeployDatabaseInSingleUserMode=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies if all DDL triggers will be disabled for the duration of the deployment operation and then re-enabled after all changes are applied.')]
        [boolean]$DisableAndReenableDdlTriggers=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether items configured for Change Data Capture (CDC) should be altered during deployment.')]
        [boolean]$DoNotAlterChangeDataCaptureObjects=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether items configured for Replication should be altered during deployment.')]
        [boolean]$DoNotAlterReplicatedObjects=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to drop all constraints that do not exist in the source model.')]
        [boolean]$DropConstraintsNotInSource=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to drop all DML triggers that do not exist in the source model.')]
        [boolean]$DropDmlTriggersNotInSource=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to drop all extended properties that do not exist in the source model.')]
        [boolean]$DropExtendedPropertiesNotInSource=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to drop all indexes that do not exist in the source model.')]
        [boolean]$DropIndexesNotInSource=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether objects that exist in the target but not source should be dropped during deployment.')]
        [boolean]$DropObjectsNotInSource=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to drop all permissions that do not exist in the source model.')]
        [boolean]$DropPermissionsNotInSource=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to drop all role memberships that do not exist in the source model.')]
        [boolean]$DropRoleMembersNotInSource=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether statistics that do not exist in the database snapshot (.dacpac) file will be dropped from the target database when you a database.')]
        [boolean]$DropStatisticsNotInSource=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether default values should be generated to populate NULL columns that are constrained to NOT NULL values.')]
        [boolean]$GenerateSmartDefaults=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the ANSI_NULL option from consideration when comparing the source and target model.')]
        [boolean]$IgnoreAnsiNulls=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the AUTHORIZATION option from consideration when comparing the source and target model.')]
        [boolean]$IgnoreAuthorizer=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the collation specifier from consideration when comparing the source and target model.')]
        [boolean]$IgnoreColumnCollation=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude comments from consideration when comparing the source and target model.')]
        [boolean]$IgnoreComments=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether differences in table column order should be ignored or updated when you publish to a database.')]
        [boolean]$IgnoreColumnOrder=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the file specification of a cryptographic provider from consideration when comparing the source and target model.')]
        [boolean]$IgnoreCryptographicProviderFilePath=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude DDL trigger order from consideration when comparing the source and target model.')]
        [boolean]$IgnoreDdlTriggerOrder=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude DDL trigger state from consideration when comparing the source and target model.')]
        [boolean]$IgnoreDdlTriggerState=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the DEFAULT_SCHEMA option from consideration when comparing the source and target model.')]
        [boolean]$IgnoreDefaultSchema=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude DML trigger order from consideration when comparing the source and target model.')]
        [boolean]$IgnoreDmlTriggerOrder=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude DML trigger state from consideration when comparing the source and target model.')]
        [boolean]$IgnoreDmlTriggerState=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude all extended properties from consideration when comparing the source and target model.')]
        [boolean]$IgnoreExtendedProperties=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the FILENAME option of FILE objects from consideration when comparing the source and target model.')]
        [boolean]$IgnoreFileAndLogFilePath=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the filegroup specifier from consideration when comparing the source and target model.')]
        [boolean]$IgnoreFilegroupPlacement=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the SIZE option of FILE objects from consideration when comparing the source and target model.')]
        [boolean]$IgnoreFileSize=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the FILLFACTOR option from consideration when comparing the source and target model.')]
        [boolean]$IgnoreFillFactor=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the path specification of FULLTEXT CATALOG objects from consideration when comparing the source and target model.')]
        [boolean]$IgnoreFullTextCatalogFilePath=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the seed value of IDENTITY columns from consideration when comparing the source and target model.')]
        [boolean]$IgnoreIdentitySeed=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the increment value of IDENTITY columns from consideration when comparing the source and target model.')]
        [boolean]$IgnoreIncrement=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude differences in index options from consideration when comparing the source and target model.')]
        [boolean]$IgnoreIndexOptions=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the PAD_INDEX option from consideration when comparing the source and target model.')]
        [boolean]$IgnoreIndexPadding=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude difference in the casing of keywords from consideration when comparing the source and target model.')]
        [boolean]$IgnoreKeywordCasing=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the ALLOW_ROW_LOCKS and ALLOW_PAGE_LOGKS options from consideration when comparing the source and target model.')]
        [boolean]$IgnoreLockHintsOnIndexes=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the SID option of the LOGIN object from consideration when comparing the source and target model.')]
        [boolean]$IgnoreLoginSids=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the NOT FOR REPLICATION option from consideration when comparing the source and target model.')]
        [boolean]$IgnoreNotForReplication=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the partition scheme object from consideration when comparing the source and target model '+
        'for the following objects: Table, Index, Unique Key, Primary Key, and Queue.')]
        [boolean]$IgnoreObjectPlacementOnPartitionScheme=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the parameter type and boundary VALUES of a PARTITION FUNCTION from consideration when '+
        'comparing the source and target model. Also excludes FILEGROUP and partition function of a PARTITION SCHEMA from consideration when comparing the source and target model.')]
        [boolean]$IgnorePartitionSchemes=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude all permission statements from consideration when comparing the source and target model.')]
        [boolean]$IgnorePermissions=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the QUOTED_IDENTIFIER option from consideration when comparing the source and target model.')]
        [boolean]$IgnoreQuotedIdentifiers=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude all ROLE MEMBERSHIP objects from consideration when comparing the source and target model.')]
        [boolean]$IgnoreRoleMembership=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the LIFETIME option of ROUTE objects from consideration when comparing the source and target model.')]
        [boolean]$IgnoreRouteLifetime=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the existence or absence of semi-colons from consideration when comparing the source and target model.')]
        [boolean]$IgnoreSemicolonBetweenStatements=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether the options on the target table are updated to match the source table.')]
        [boolean]$IgnoreTableOptions=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude user settings from consideration when comparing the source and target model.')]
        [boolean]$IgnoreUserSettingsObjects=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude whitespace from consideration when comparing the source and target model.')]
        [boolean]$IgnoreWhitespace=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the CHECK|NO CHECK option of a CHECK constraint object from consideration when comparing the source and target model.')]
        [boolean]$IgnoreWithNocheckOnCheckConstraints=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to exclude the CHECK|NO CHECK option of a FOREIGN KEY constraint object from consideration when comparing the source and target model.')]
        [boolean]$IgnoreWithNocheckOnForeignKeys=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to include referenced, external elements that also compose the source model and then update the target database in a single deployment operation.')]
        [boolean]$IncludeCompositeObjects=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to use transations during the deployment operation and commit the transaction after all changes are successfully applied.')]
        [boolean]$IncludeTransactionalScripts=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether to force a change to CLR assemblies by dropping and recreating them.')]
        [boolean]$NoAlterStatementsToChangeClrTypes=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether files are supplied for filegroups defined in the deployment source.')]
        [boolean]$PopulateFilesOnFileGroups=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether the database will be registered as a Data-Tier Application. If the target database is already a registered Data-Tier Application, then the registration will be updated.')]
        [boolean]$RegisterDataTierApplication=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether DeploymentPlanExecutor contributors should be run when other operations are executed. Default is false.')]
        [boolean]$RunDeploymentPlanExecutors=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether the target database should be altered to match the source model''s collation.')]
        [boolean]$ScriptDatabaseCollation=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether the target database should be altered to match the source model''s compatibility level.')]
        [boolean]$ScriptDatabaseCompatibility=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether the database options in the target database should be updated to match the source model.')]
        [boolean]$ScriptDatabaseOptions=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether the target database should be checked to ensure that it exists, is online and can be updated.')]
        [boolean]$ScriptDeployStateChecks=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether a file size is specified when adding files to file groups.')]
        [boolean]$ScriptFileSize=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether constraints are validated after all changes are applied.')]
        [boolean]$ScriptNewConstraintValidation=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether referencing procedures are refreshed when referenced objects are updated.')]
        [boolean]$ScriptRefreshModule=$false,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether the deployment operation should proceed when errors are generated during plan verification.')]
        [boolean]$TreatVerificationErrorsAsWarnings=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether the deployment operation should proceed when errors are generated during plan verification.')]
        [boolean]$UnmodifiableObjectWarnings=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether deployment will verify that the collation specified in the source model is compatible with the collation specified in the target model.')]
        [boolean]$VerifyCollationCompatibility=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies whether the plan verification phase is executed or not.')]
        [boolean]$VerifyDeployment=$true,
        [Parameter(Mandatory=$false,HelpMessage='Specifies the semicolon-separated list of objects to ignore, when not found in source. Default is Logins,ServerAuditSpecifications,RoleMembership,ServerRoleMembership')]
        [AllowEmptyString()]
        [string]$DoNotDropObjectTypes='Logins,Users,ServerAuditSpecifications,RoleMembership,ServerRoleMembership',
        [Parameter(Mandatory=$false,HelpMessage='Specifies the semicolon-separated list of objects to exclude from deployment, when not found in source. Default is Logins,Users,ServerAuditSpecifications,RoleMembership,ServerRoleMembership')]
        [AllowEmptyString()]
        [string]$ExcludeObjectTypes="Logins,Users,ServerAuditSpecifications,RoleMembership,ServerRoleMembership"
    )
    
    if (-not(get-module Microsoft.PowerShell.Archive)) {
    	Write-Verbose "Attempting to import the following package: Microsoft.PowerShell.Archive"
    	import-module Microsoft.PowerShell.Archive
    }
    
    $versionsToTry=@()
    foreach($v in 20..11) {
        $p="${env:ProgramFiles}\Microsoft SQL Server\${v}0\DAC\bin"
        if (Test-Path -Path "${p}\SqlPackage.exe") { $versionsToTry+=$p } 
        else { 
            $p="${env:ProgramFiles(x86)}\Microsoft SQL Server\${v}0\DAC\bin";
            if (Test-Path -Path "${p}\SqlPackage.exe") { $versionsToTry+=$p } 
        }
    }
    $dacFxPath=$versionsToTry|Select-Object -First 1
	if ($null -eq $dacFxPath) {
    	throw [Exception]::new('DACPAC is not found. Please install the tool. https://go.microsoft.com/fwlink/?linkid=2143544');
    }
    $curLocation = Get-Location
    try {
        # Create a connection to the server  
        $scriptPath="${outputPath}\${databaseName}_deployment.sql"
        $reportPath="${outputPath}\${databaseName}_report.xml"

        #build arguments
        #mandatory parameters
        $argumentList= @("/a:$pkgAction","`"/dsp:$scriptPath`"","`"/drp:$reportPath`"","`"/sf:$dacpacPath`"","`"/tsn:$server`"","`"/tdn:$databaseName`"","/TargetEncryptConnection:False")

        $argumentList+="/p:AllowDropBlockingAssemblies=$AllowDropBlockingAssemblies"
        $argumentList+="/p:AllowIncompatiblePlatform=$AllowIncompatiblePlatform"
        $argumentList+="/p:BackupDatabaseBeforeChanges=$BackupDatabaseBeforeChanges"
        $argumentList+="/p:BlockOnPossibleDataLoss=$BlockOnPossibleDataLoss"
        $argumentList+="/p:BlockWhenDriftDetected=$BlockWhenDriftDetected"
        $argumentList+="/p:CommentOutSetVarDeclarations=$CommentOutSetVarDeclarations"
        $argumentList+="/p:CompareUsingTargetCollation=$CompareUsingTargetCollation"
        $argumentList+="/p:CreateNewDatabase=$CreateNewDatabase"
        $argumentList+="/p:DeployDatabaseInSingleUserMode=$DeployDatabaseInSingleUserMode"
        $argumentList+="/p:DisableAndReenableDdlTriggers=$DisableAndReenableDdlTriggers"
        $argumentList+="/p:DoNotAlterChangeDataCaptureObjects=$DoNotAlterChangeDataCaptureObjects"
        $argumentList+="/p:DoNotAlterReplicatedObjects=$DoNotAlterReplicatedObjects"
        $argumentList+="/p:DropConstraintsNotInSource=$DropConstraintsNotInSource"
        $argumentList+="/p:DropDmlTriggersNotInSource=$DropDmlTriggersNotInSource"
        $argumentList+="/p:DropExtendedPropertiesNotInSource=$DropExtendedPropertiesNotInSource"
        $argumentList+="/p:DropIndexesNotInSource=$DropIndexesNotInSource"
        $argumentList+="/p:DropObjectsNotInSource=$DropObjectsNotInSource"
        $argumentList+="/p:DropPermissionsNotInSource=$DropPermissionsNotInSource"
        $argumentList+="/p:DropStatisticsNotInSource=$DropStatisticsNotInSource"
        $argumentList+="/p:DropRoleMembersNotInSource=$DropRoleMembersNotInSource"
        $argumentList+="/p:GenerateSmartDefaults=$GenerateSmartDefaults"
        $argumentList+="/p:IgnoreAnsiNulls=$IgnoreAnsiNulls"
        $argumentList+="/p:IgnoreAuthorizer=$IgnoreAuthorizer"
        $argumentList+="/p:IgnoreColumnCollation=$IgnoreColumnCollation"
        $argumentList+="/p:IgnoreComments=$IgnoreComments"
        $argumentList+="/p:IgnoreColumnOrder=$IgnoreColumnOrder"
        $argumentList+="/p:IgnoreCryptographicProviderFilePath=$IgnoreCryptographicProviderFilePath"
        $argumentList+="/p:IgnoreDdlTriggerOrder=$IgnoreDdlTriggerOrder"
        $argumentList+="/p:IgnoreDdlTriggerState=$IgnoreDdlTriggerState"
        $argumentList+="/p:IgnoreDefaultSchema=$IgnoreDefaultSchema"
        $argumentList+="/p:IgnoreDmlTriggerOrder=$IgnoreDmlTriggerOrder"
        $argumentList+="/p:IgnoreDmlTriggerState=$IgnoreDmlTriggerState"
        $argumentList+="/p:IgnoreExtendedProperties=$IgnoreExtendedProperties"
        $argumentList+="/p:IgnoreFileAndLogFilePath=$IgnoreFileAndLogFilePath"
        $argumentList+="/p:IgnoreFilegroupPlacement=$IgnoreFilegroupPlacement"
        $argumentList+="/p:IgnoreFileSize=$IgnoreFileSize"
        $argumentList+="/p:IgnoreFillFactor=$IgnoreFillFactor"
        $argumentList+="/p:IgnoreFullTextCatalogFilePath=$IgnoreFullTextCatalogFilePath"
        $argumentList+="/p:IgnoreIdentitySeed=$IgnoreIdentitySeed"
        $argumentList+="/p:IgnoreIncrement=$IgnoreIncrement"
        $argumentList+="/p:IgnoreIndexOptions=$IgnoreIndexOptions"
        $argumentList+="/p:IgnoreIndexPadding=$IgnoreIndexPadding"
        $argumentList+="/p:IgnoreKeywordCasing=$IgnoreKeywordCasing"
        $argumentList+="/p:IgnoreLockHintsOnIndexes=$IgnoreLockHintsOnIndexes"
        $argumentList+="/p:IgnoreLoginSids=$IgnoreLoginSids"
        $argumentList+="/p:IgnoreNotForReplication=$IgnoreNotForReplication"
        $argumentList+="/p:IgnoreObjectPlacementOnPartitionScheme=$IgnoreObjectPlacementOnPartitionScheme"
        $argumentList+="/p:IgnorePartitionSchemes=$IgnorePartitionSchemes"
        $argumentList+="/p:IgnorePermissions=$IgnorePermissions"
        $argumentList+="/p:IgnoreQuotedIdentifiers=$IgnoreQuotedIdentifiers"
        $argumentList+="/p:IgnoreRoleMembership=$IgnoreRoleMembership"
        $argumentList+="/p:IgnoreRouteLifetime=$IgnoreRouteLifetime"
        $argumentList+="/p:IgnoreSemicolonBetweenStatements=$IgnoreSemicolonBetweenStatements"
        $argumentList+="/p:IgnoreTableOptions=$IgnoreTableOptions"
        $argumentList+="/p:IgnoreUserSettingsObjects=$IgnoreUserSettingsObjects"
        $argumentList+="/p:IgnoreWhitespace=$IgnoreWhitespace"
        $argumentList+="/p:IgnoreWithNocheckOnCheckConstraints=$IgnoreWithNocheckOnCheckConstraints"
        $argumentList+="/p:IgnoreWithNocheckOnForeignKeys=$IgnoreWithNocheckOnForeignKeys"
        $argumentList+="/p:IncludeCompositeObjects=$IncludeCompositeObjects"
        $argumentList+="/p:IncludeTransactionalScripts=$IncludeTransactionalScripts"
        $argumentList+="/p:NoAlterStatementsToChangeClrTypes=$NoAlterStatementsToChangeClrTypes"
        $argumentList+="/p:PopulateFilesOnFileGroups=$PopulateFilesOnFileGroups"
        $argumentList+="/p:RegisterDataTierApplication=$RegisterDataTierApplication"
        $argumentList+="/p:RunDeploymentPlanExecutors=$RunDeploymentPlanExecutors"
        $argumentList+="/p:ScriptDatabaseCollation=$ScriptDatabaseCollation"
        $argumentList+="/p:ScriptDatabaseCompatibility=$ScriptDatabaseCompatibility"
        $argumentList+="/p:ScriptDatabaseOptions=$ScriptDatabaseOptions"
        $argumentList+="/p:ScriptDeployStateChecks=$ScriptDeployStateChecks"
        $argumentList+="/p:ScriptFileSize=$ScriptFileSize"
        $argumentList+="/p:ScriptNewConstraintValidation=$ScriptNewConstraintValidation"
        $argumentList+="/p:ScriptRefreshModule=$ScriptRefreshModule"
        $argumentList+="/p:TreatVerificationErrorsAsWarnings=$TreatVerificationErrorsAsWarnings"
        $argumentList+="/p:UnmodifiableObjectWarnings=$UnmodifiableObjectWarnings"
        $argumentList+="/p:VerifyCollationCompatibility=$VerifyCollationCompatibility"
        $argumentList+="/p:VerifyDeployment=$VerifyDeployment"


        # list of objects to ignore, when not found in source
        if ($DoNotDropObjectTypes -ne '') {
            $argumentList+="`"/p:DoNotDropObjectTypes=$DoNotDropObjectTypes`""
        }
        # list of objects to exclude completely from deployment
        if ($ExcludeObjectTypes -ne '') {
            $argumentList+="`"/p:ExcludeObjectTypes=$ExcludeObjectTypes`""
        }
        
        $argumentList+="/p:Storage=File"

        foreach($item in $sqlCmdVars.Keys) {
            $val = $sqlCmdVars[$item];
            $argumentList+="`"/v:$item=$val`""
        }

        $argumentList+=$additionalParams

        $cmdLine = $argumentList -join " "
        Set-Location $dacFxPath
        Write-Verbose "Found DACPAC utility at $dacFxPath\sqlPackage.exe"
        Write-Verbose "Running the packagage using the following command line: $cmdLine"
        Invoke-Expression -Command ".\sqlPackage.exe $cmdLine"
        #Start-Process -FilePath "$dacFxPath\sqlPackage.exe" -ArgumentList $cmdLine
        if ($pkgAction -eq "Script") {
          $scptText = Get-Content $scriptPath
          $scptText = ($scptText|ForEach-Object { if ($_.StartsWith("REVOKE CONNECT ")) { "--"+$_; } else {$_;} })
          Set-Content -Path $scriptPath -Value $scptText
        }
        if ($attachAsArtefacts) {
            $envDescr = $OctopusParameters['Octopus.Environment.Name']
            if (Test-Path -Path $scriptPath) {
                New-OctopusArtifact -Path $scriptPath -Name "$envDescr-$databaseName-deploymentScript.sql"
            } else {
                Write-Warning "No deployment file found at $scriptPath"
            }
            if (Test-Path -Path $reportPath) {
                New-OctopusArtifact -Path $reportPath -Name "$envDescr-$databaseName-report.xml"
            } else {
                Write-Warning "No report file found at $reportPath"
            }
        }
        
        Set-Location $curLocation
    } catch {
        Set-Location $curLocation
        Write-Host "There is an error occurred."
        Write-Host $_
        # rethrow the exception to fail the script
        throw $_.Exception
    }
}
