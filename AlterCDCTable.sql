/*
The procedure alters the set of columns to capture by Change Data Capture, while taking care of the data in other CDC instance for the table.
1. creates a new CDC capture instance (or drops it, if no fields were provided.)
2. moves the matching fields from the old capture instance to the new one while taking care of the $update_mask field. (so if the f1 field was in the second position, it resulted in the 0x02 mask. If it has a different position (e.g. the third) now, the corresponding mask will show 0x04)
3. creates the synonyms for the CDC functions, so the CDC-related logic does not need to care of the changes in the capture instance names.
4. drops the old CDC instance
*/
CREATE OR ALTER PROCEDURE dbo.AlterCDCTable
    @source_name sysname,
    @captured_column_list NVARCHAR(MAX),
    @source_schema sysname = 'dbo',
    @capture_instance sysname = NULL,
    @role_name sysname = NULL,
    @supports_net_changes BIT = NULL,
    @index_name sysname = NULL,
    @filegroup_name sysname = NULL,
    @allow_partition_switch BIT = NULL,
    @suppressMessages BIT = 0    
WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;

    SET @suppressMessages = ISNULL(@suppressMessages, 0);

    -- check if the database is CDC-enabled
    DECLARE @is_cdc_enabled BIT;
    SELECT @is_cdc_enabled = is_cdc_enabled
    FROM sys.databases
    WHERE name = DB_NAME();

    IF @is_cdc_enabled IS NULL
       OR @is_cdc_enabled = 0
        THROW 50000, 'The database does not exists or CDC is not enabled for it. No changes is made.', 16;

    IF @captured_column_list IS NULL
        THROW 50000, 'The list of columns to capture is null. No changes is made.', 16;

    -- CDC has been enabled for the table. validate the input parameters
    DECLARE @table_object_id INT;
    SELECT @table_object_id = tb.object_id,
           @is_cdc_enabled = tb.is_tracked_by_cdc
    FROM sys.tables tb
        INNER JOIN sys.schemas s
            ON s.schema_id = tb.schema_id
    WHERE s.name = @source_schema
          AND tb.name = @source_name;

    IF @table_object_id IS NULL
        THROW 50000, 'The source table is not found or you don''t have permissions to access it. No changes is made.', 16;
    -- craft the common part of the cdc-related entities
    DECLARE @cdcTableNameBase sysname = @source_schema + N'_' + @source_name;

    -- list of variables for the system CDC procedures
    DECLARE @cdc_role_name sysname,
            @cdc_index_name sysname,
            @cdc_filegroup_name sysname,
            @old_capture_instance sysname,
            @cdc_capture_instance sysname,
            @cdc_supports_net_changes BIT,
			@cdc_allow_partition_switch BIT,
            @cdc_table_id INT;

    -- cdc may or may not be enabled for the database. use the standard functions to avoid the compilation errors.
    -- the tables below will hold the record sets of those functions.
    DECLARE @cdc_change_table TABLE
    (
        source_schema sysname NOT NULL,
        source_table sysname NOT NULL,
        capture_instance sysname NOT NULL,
        object_id INT NOT NULL,
        source_object_id INT NULL,
        start_lsn BINARY(10) NULL,
        end_lsn BINARY(10) NULL,
        supports_net_changes BIT NULL,
        has_drop_pending BIT NULL,
        role_name sysname NULL,
        index_name sysname NULL,
        filegroup_name sysname NULL,
        create_date DATETIME NULL,
        index_column_list NVARCHAR(MAX) NULL,
        captured_column_list NVARCHAR(MAX) NULL
    );
    DECLARE @oldColumnList TABLE
    (
        source_schema sysname NOT NULL,
        source_table sysname NOT NULL,
        capture_instance sysname NOT NULL,
        column_name sysname NOT NULL,
        column_id INT NOT NULL,
        column_ordinal INT NOT NULL,
        data_type sysname NOT NULL,
        character_maximum_length INT NULL,
        numeric_precision TINYINT NULL,
        numeric_precision_radix SMALLINT NULL,
        numeric_scale INT NULL,
        datetime_precision SMALLINT NULL
    );
    DECLARE @newColumnList TABLE
    (
        source_schema sysname NOT NULL,
        source_table sysname NOT NULL,
        capture_instance sysname NOT NULL,
        column_name sysname NOT NULL,
        column_id INT NOT NULL,
        column_ordinal INT NOT NULL,
        data_type sysname NOT NULL,
        character_maximum_length INT NULL,
        numeric_precision TINYINT NULL,
        numeric_precision_radix SMALLINT NULL,
        numeric_scale INT NULL,
        datetime_precision SMALLINT NULL
    );
    IF @is_cdc_enabled = 1
    BEGIN
        IF @suppressMessages = 0
            PRINT 'Table is under control of CDC';

        -- read the current setup for the table	
        INSERT INTO @cdc_change_table
        EXEC sys.sp_cdc_help_change_data_capture @source_schema = @source_schema,
                                                 @source_name = @source_name;

        SELECT TOP (1)
               @cdc_table_id = object_id,
               @cdc_role_name = role_name,
               @cdc_filegroup_name = filegroup_name,
               @old_capture_instance = capture_instance,
               @cdc_supports_net_changes = supports_net_changes,
               @cdc_index_name = index_name
        FROM @cdc_change_table
        ORDER BY object_id DESC;

        IF @suppressMessages = 0
            PRINT 'Current setup is: capture_instance:' + ISNULL(@old_capture_instance, '<undefined>') + ', role_name:'
                  + ISNULL(@cdc_role_name, '<undefined>') + ', filegroup_name:'
                  + ISNULL(@cdc_filegroup_name, '<undefined>') + ', index_name:'
                  + ISNULL(@cdc_index_name, '<undefined>');

        IF @old_capture_instance IS NOT NULL
        BEGIN
            -- check how many capture tables there. there must be only one
            IF EXISTS
            (
                SELECT NULL
                FROM @cdc_change_table
                HAVING MIN(object_id) <> MAX(object_id)
            )
                THROW 50000, 'The maximum (2) of CDC instances per table has been reached. Please ensure there is only  one CDC instance exists. No changes is made.', 16;

            -- read the captured columns as they are atm.
            INSERT INTO @oldColumnList
            EXEC sys.sp_cdc_get_captured_columns @capture_instance = @old_capture_instance; -- sysname
        END;
    END;
    ELSE -- cdc is NOT enabled for the table
    BEGIN
        IF @suppressMessages = 0
            PRINT 'Table is NOT under control of CDC';
    END;

    -- validate the list of columns
    DECLARE @proposedColumns TABLE
    (
        cname sysname NOT NULL
    );
    IF @captured_column_list <> ''
    BEGIN
        DECLARE @Index INT = 1,
                @Slice NVARCHAR(MAX),
                @String NVARCHAR(MAX) = @captured_column_list;

        --SET @Index = 1;
        --SET @String = @captured_column_list;
        WHILE @Index <> 0
        BEGIN
            -- Get the Index of the first occurence of the Split character
            SELECT @Index = CHARINDEX(',', @String);

            -- NOW PUSH EVERYTHING TO THE LEFT OF IT INTO THE SLICE VARIABLE
            IF @Index <> 0
                SELECT @Slice = LEFT(@String, @Index - 1);
            ELSE
                SELECT @Slice = @String;

            INSERT INTO @proposedColumns
            (
                cname
            )
            SELECT name
            FROM sys.columns
            WHERE object_id = @table_object_id
                  AND name = @Slice;

            IF @@ROWCOUNT = 0
            BEGIN
                SET @String
                    = N'The field ' + @Slice + N' has not been found in the source table ' + @source_name
                      + N'. The process has been aborted and no changes is made.';
                THROW 50000, @String, 16;
            END;

            -- CHOP THE ITEM REMOVED OFF THE MAIN STRING
            SET @String = RIGHT(@String, LEN(@String) - @Index);

            -- BREAK OUT IF WE ARE DONE
            IF LEN(@String) = 0
                BREAK;
        END;

        -- obtain the possible capture instance name
        SET @Index = 0;
        SET @cdc_capture_instance = COALESCE(@capture_instance, @cdcTableNameBase);

        WHILE EXISTS
    (
        SELECT NULL
        FROM sys.tables tb
            INNER JOIN sys.schemas s
                ON s.schema_id = tb.schema_id
        WHERE s.name = 'cdc'
              AND tb.name = @cdc_capture_instance + '_CT'
    )
              AND @Index < 1000
        BEGIN
            SET @Index += 1;
            SET @cdc_capture_instance = @cdcTableNameBase + N'_v' + CONVERT(sysname, @Index);
        END;

        IF @Index = 1000
            THROW 50000, 'Too many attempts to find the suitable capture instance for the table. No changes is made.', 16;
        IF @suppressMessages = 0
            PRINT 'The new capture instance is ' + @cdc_capture_instance;
    END;
    ELSE -- the CDC table does not exist and no fields required to capture. exiting.
    IF @is_cdc_enabled = 0
       OR @is_cdc_enabled IS NULL
    BEGIN
        IF @suppressMessages = 0
            PRINT 'There is nothing to do. Exiting.';
        RETURN 0;
    END;

    -- calculate how many fields is added and how many is removed.
    DECLARE @columnsDropped INT,
            @columnsRemained INT,
            @columnsAdded INT;
    SELECT @columnsDropped = SUM(IIF(ncl.cname IS NULL AND ocl.column_name IS NOT NULL, 1, 0)),
           @columnsRemained = SUM(IIF(ncl.cname IS NOT NULL AND ocl.column_name IS NOT NULL, 1, 0)),
           @columnsAdded = SUM(IIF(ncl.cname IS NOT NULL AND ocl.column_name IS NULL, 1, 0))
    FROM @oldColumnList ocl
        FULL JOIN @proposedColumns ncl
            ON ncl.cname = ocl.column_name;
    -- check for the particular cases
    IF @columnsDropped = 0 -- nothing to drop
       AND @columnsAdded = 0 -- nothing to add
    BEGIN
        IF @suppressMessages = 0
            PRINT 'The column list remains the same. There is nothing to do. Exiting.';
        RETURN 0;
    END;

    -- passing the NULLs in those variables to sp_cdc_enable_table to complain
    SET @cdc_role_name = COALESCE(@role_name, @cdc_role_name);
    SET @cdc_supports_net_changes = COALESCE(@supports_net_changes, @cdc_supports_net_changes);
    SET @cdc_filegroup_name = COALESCE(@filegroup_name, @cdc_filegroup_name);
    SET @cdc_allow_partition_switch = COALESCE(@allow_partition_switch,@cdc_allow_partition_switch);
    SET @cdc_index_name = COALESCE(@index_name,@cdc_index_name);

    -- sanity check:
    IF @columnsRemained > 0
       AND @old_capture_instance IS NULL
        THROW 50000, 'Internal error', 16;

    IF @supports_net_changes = 1
    BEGIN
        IF @cdc_index_name IS NULL
        BEGIN
            -- for the net changes we need a name of the unique index 
            SELECT TOP (1)
                   @cdc_index_name = i.name
            FROM sys.indexes i
                INNER JOIN
                (
                    SELECT object_id,
                           index_id,
                           COUNT(1) AS columns_in_index
                    FROM sys.index_columns
                    WHERE is_included_column = 0
                    GROUP BY object_id,
                             index_id
                ) ic
                    ON ic.index_id = i.index_id
                       AND ic.object_id = i.object_id
            WHERE i.object_id = @table_object_id
                  AND i.is_unique = 1
                  AND i.is_disabled = 0
            ORDER BY i.is_primary_key DESC,
                     i.auto_created ASC,
                     i.type ASC,
                     ic.columns_in_index;

            IF @cdc_index_name IS NULL
                THROW 50000, 'Unable to determine the unique index for the net changes. No changes is made.', 16;
        END;
        IF @suppressMessages = 0
            PRINT 'The following index will be used to uniquely identify rows in the source table: ' + @cdc_index_name;
    END;
    -- if there is something to change - create a new CDC instance and redirect the corresponding synonyms
    DECLARE @res INT,
            @sql NVARCHAR(4000),
            @fn_netchanges_name NVARCHAR(4000)
        =   QUOTENAME(@source_schema) + N'.' + QUOTENAME(N'fn_cdc_get_net_changes_' + @cdcTableNameBase),
            @fn_allchanges_name NVARCHAR(4000) = QUOTENAME(@source_schema) + N'.'
                                                 + QUOTENAME(N'fn_cdc_get_all_changes_' + @cdcTableNameBase);
    BEGIN TRANSACTION;
    BEGIN TRY
        IF OBJECT_ID(@fn_allchanges_name, 'SN') IS NOT NULL
        BEGIN
            IF @suppressMessages = 0
                PRINT 'Dropping the ALL_CHANGES synonym: ' + @fn_allchanges_name;

            SET @sql = N'DROP SYNONYM ' + @fn_allchanges_name;
            EXEC sp_executesql @sql;
        END;


        IF OBJECT_ID(@fn_netchanges_name, 'SN') IS NOT NULL
        BEGIN
            IF @suppressMessages = 0
                PRINT 'Dropping the NET_CHANGES synonym: ' + @fn_netchanges_name;

            SET @sql = N'DROP SYNONYM ' + @fn_netchanges_name;
            EXEC sp_executesql @sql;
        END;

        IF @columnsRemained + @columnsAdded > 0
        BEGIN
            EXEC @res = sys.sp_cdc_enable_table @source_schema = @source_schema,
                                                @source_name = @source_name,
                                                @capture_instance = @cdc_capture_instance,
                                                @supports_net_changes = @supports_net_changes,
                                                @role_name = @cdc_role_name,
                                                @index_name = @cdc_index_name,
                                                @captured_column_list = @captured_column_list,
                                                @filegroup_name = @cdc_filegroup_name,
                                                @allow_partition_switch = @cdc_allow_partition_switch;

            IF @res <> 0
                THROW 50000, 'Unexpected error in sys.sp_cdc_enable_table', 16;

            IF @suppressMessages = 0
                PRINT 'Creating the ALL_CHANGES synonym: ' + @fn_allchanges_name;
            SET @sql
                = N'CREATE SYNONYM ' + @fn_allchanges_name + N' FOR cdc.'
                  + QUOTENAME('fn_cdc_get_all_changes_' + @cdc_capture_instance);
            EXEC sp_executesql @sql;

            IF @cdc_supports_net_changes = 1
            BEGIN
                IF @suppressMessages = 0
                    PRINT 'Creating the NET_CHANGES synonym: ' + @fn_netchanges_name;
                SET @sql
                    = N'CREATE SYNONYM ' + @fn_netchanges_name + N' FOR cdc.'
                      + QUOTENAME('fn_cdc_get_net_changes_' + @cdc_capture_instance);

                EXEC sp_executesql @sql;
            END;

        END;

        -- move all records of the previous CDC instance to the new one.
        IF @columnsRemained > 0
        BEGIN
            -- refill the table with the actual list of captured columns
            INSERT INTO @newColumnList
            EXEC sys.sp_cdc_get_captured_columns @capture_instance = @cdc_capture_instance;


            DECLARE @listofcommonfields NVARCHAR(MAX) = N'__$start_lsn,__$end_lsn,__$seqval,__$operation',
                    @formula NVARCHAR(MAX) = N'';

            SELECT @formula
                = STRING_AGG('CAST((' + parts.part + ') as varbinary(1))', '+') WITHIN GROUP(ORDER BY parts.grp DESC)
            FROM
            (
                SELECT grp,
                       STRING_AGG(chunks.part, N'|') AS part
                FROM
                (
                    SELECT DISTINCT
                           ncl.column_ordinal / 8 AS grp,
                           IIF(ocl.column_ordinal IS NOT NULL,
                               N'IIF(__$operation IN (3,4) and sys.fn_cdc_is_bit_set('
                               + CONVERT(NVARCHAR(10), ocl.column_ordinal) + N',__$update_mask)=0,0,'
                               + CONVERT(NVARCHAR(10), POWER(2, -1 + (ncl.column_ordinal % 8))) + N')',
                               '0') AS part
                    FROM @newColumnList ncl
                        LEFT JOIN @oldColumnList ocl
                            ON ocl.column_name = ncl.column_name
                ) AS chunks
                GROUP BY grp
            ) AS parts;


            SELECT @listofcommonfields = @listofcommonfields + N',' + ncl.column_name
            FROM @oldColumnList ocl
                INNER JOIN @newColumnList ncl
                    ON ncl.column_name = ocl.column_name;

            SET @listofcommonfields = @listofcommonfields + N',__$command_id';

            IF @suppressMessages = 0
                PRINT 'Copying retained columns from one table to the other';

            SET @sql
                = N'INSERT INTO cdc.' + QUOTENAME(@cdc_capture_instance + N'_CT') + N'(' + @listofcommonfields
                  + N',__$update_mask) SELECT ' + @listofcommonfields + N', IIF(__$update_mask is not null,' + @formula
                  + N',null) as __$update_mask FROM cdc.' + QUOTENAME(@old_capture_instance + N'_CT')
                  + ';UPDATE dst SET start_lsn = newminlsn FROM cdc.change_tables dst CROSS APPLY '
                  + '(SELECT MIN(__$start_lsn) AS newminlsn FROM  cdc.' + QUOTENAME(@cdc_capture_instance + N'_CT')+') AS t '
                  + ' WHERE dst.OBJECT_ID = OBJECT_ID(''cdc.' + QUOTENAME(@cdc_capture_instance + N'_CT') + N''')'
                  + ' AND dst.start_lsn > t.newminlsn';

            IF @suppressMessages = 0
                PRINT N'The transitioning script is: ' + @sql;

            EXEC sp_executesql @sql;
        END;
        -- remove the previous capture instance
        IF @old_capture_instance IS NOT NULL
        BEGIN
            IF @suppressMessages = 0
                PRINT 'Removing the previous capture instance: ' + @old_capture_instance;
            EXEC @res = sys.sp_cdc_disable_table @source_schema = @source_schema,
                                                 @source_name = @source_name,
                                                 @capture_instance = @old_capture_instance;
            IF @res <> 0
                THROW 50000, 'Unexpected error in sys.sp_cdc_disable_table', 16;
        END;
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK;
        THROW;
    END CATCH;
END;
GO
