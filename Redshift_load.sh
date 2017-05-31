#!/bin/bash
# ------------------------------------------------------------------
# 		   Author: Randy 
#		   Redshift Loading Script
#          Copies data from HDFS to S3 and loads it in a AWS Redshift table 
# 		   Location: node where we want to run an operation
#		   The script requires parameters.
#	
# ------------------------------------------------------------------


# Usage:
#  $ ./myScript param1 param2 param3 [param4] ...
# * param1: Boolean used to determine if properties will be passed to the script or used the hardcoded properties in the file. 
# * param2: Redshift load action, one of the following: (load, upsert, loadTruncate, bulkDeleteAppend, copyHDFStoS3)
# * param3 Boolean for copying to S3 control (true / false)
# * param4: AWS Access key 
# * param5: AWS Secret key
# * param6: HDFS source path ex: /source_dir/subdir/  
# * param7: S3 Bucket name
# * param8: S3 destination path 
# * param9: Redshift Host name
# * param10: Redshift Host port
# * param11: Redshift database name
# * param12: Redshift user name
# * param13: Redshift pass
# * param14: Redshift schema
# * param15: Redshift table name
# * param16: prefix of files in s3 that will be loaded into redshift (ex: part-)
# * param17: Input file delimiter
# * param18: Input file format
# * param19: Additional copy command parameters (could be empty string)

# OPTIONAL (for Upsert Action)
# * param20:  Redshift table primary keys ex: "key1,key2,key3"
# OPTIONAL (for bulkDeleteAppend Action)
# * param20: Redshift delete append column name
# * param21: Redshift delete append column value


# Required script properties below 
# setProperties=false
# scriptAction="none"
# copyHDFStoS3=true

# Parameters to pass if setProperties=true
aws_access_key_id=key  		 # * PARAM_4
aws_secret_access_key=key  		# * PARAM_5
hdfs_source="" 		# * PARAM_6
s3_bucket=""	 		# * PARAM_7
s3_path="" 			# * PARAM_8
# Required Redshift copy properties below
db_host=bi-dev-dw.cmadjmmldhzj.us-west-2.redshift.amazonaws.com  			# * PARAM_9
db_port=5439	 			# * PARAM_10
db_name=dw 			# * PARAM_11
db_user=""			# * PARAM_12
db_pass="" 			# * PARAM_13
rsSchema=public 			# * PARAM_14
redshiftTable=table_name 			# * PARAM_15
copy_files_prefix=part 			# * PARAM_16
delim=''\'','\''' 			# * PARAM_17
rs_input_file_format=csv  			# * PARAM_18 
rs_copy_params='ACCEPTANYDATE TRUNCATECOLUMNS MAXERROR 0'    			# * PARAM_19 

# OPTIONAL PARAMETERS HERE (Depending on the script action )
# Upsert action parameter
rsUpsertPrimaryKeys="a,b"   # "key1,key2,key3"  comma separated  			# * OPTIONAL PARAM_20
# BulkDeleteAppend action 
rdDeleteAppendColumn="b"  			# * OPTIONAL PARAM_20
rsDeleteAppendValue="N"  			# * OPTIONAL PARAM_21

# Parameters deducted from inputs
copy_s3_path=s3://$s3_bucket$s3_path$copy_files_prefix   # LOGS PREFIX ex:  {bucket-name}/{optional-prefix}{distribution-ID} 
# Global parameters for distcp
proxy_host="squid.common"
proxy_port="3128"
star="*"
# Terminate execution upon error
set -e



# Copies data from HDFS to S3
function copyHDFStoS3
{

	if [[ "$1" == "true" ]]; then
		copyToS3command="hadoop distcp -Dfs.s3a.access.key=$aws_access_key_id -Dfs.s3a.secret.key=$aws_secret_access_key  -Dfs.s3a.proxy.host=$proxy_host -Dfs.s3a.proxy.port=$proxy_port -overwrite hdfs://$hdfs_source s3a://$s3_bucket/$s3_path"
		echo "$copyToS3command"
	    $copyToS3command	
	fi
}	# End of copyHDFStoS3


# Creates temp files that store logs for executed commands and sql output logs
function setupExecute
{
	cmdsGlobal=$(mktemp /tmp/cmds_redshift.XXXXXX) || { echo "Failed to create temp commands file"; exit 1; }
	logsGlobal=$(mktemp /tmp/logs_redshift.XXXXXX) || { echo "Failed to create temp logs file"; exit 1; }

} # End of setupExecute

## 
# Executes a redshift sql statements. The statement is stored in a temp file for execution and also in another log file.
# * param1: Redshift sql statement  
function executeStatement
{
	export PGPASSWORD=$db_pass
	commandToExecute="$1"
	echo "$commandToExecute" >> $cmdsGlobal
	currCmd=$(mktemp /tmp/curr_cmds_redshift.XXXXXX) || { echo "Failed to create temp current commands file"; exit 1; }
	echo "$commandToExecute" >> $currCmd

	psql -d $db_name -h $db_host -p $db_port -U $db_user -f $currCmd >> $logsGlobal 2>&1	
	rm $currCmd 	# drop the temp command file
} # End of executeStatement


# Creates and loads a staging table given a staging table name.
# * param1: Staging table name
function createLoadStaging
{
	#1. First create a staging table and copy the data
	local staging_rs_table="$1"
	local main_rs_table="$rsSchema.$redshiftTable" 

	local createStagingSqlStatement="create table $staging_rs_table as select * from  $main_rs_table limit 0;" 
	executeStatement "$createStagingSqlStatement"

	#2. Copy all data into the staging table
	local copyStatement="copy $staging_rs_table from '$copy_s3_path' CREDENTIALS 'aws_access_key_id=$aws_access_key_id;aws_secret_access_key=$aws_secret_access_key' DELIMITER $delim $rs_input_file_format acceptinvchars ACCEPTANYDATE BLANKSASNULL EMPTYASNULL TRUNCATECOLUMNS MAXERROR 0 ;"
	executeStatement "$copyStatement"
}  # End of createLoadStaging

function checkSuccess()
{
    echo "executed commands:"
    echo "$(cat ${cmdsGlobal})"

    echo "execution logs:"
    echo "$(cat ${logsGlobal})"

	if grep -q ERROR "$logsGlobal"; then
   		rm ${cmdsGlobal}
        rm ${logsGlobal}
   		echo "FAILED"
   		exit 1
 	else
        rm ${cmdsGlobal}
        rm ${logsGlobal}
 		echo "SUCCESS"
 	fi
}

##
# Copy data from S3 into a Redshift table
#
function loadRedshiftTable
{
	local loadRedshiftStatement="copy $rsSchema.$redshiftTable from '$copy_s3_path' CREDENTIALS 'aws_access_key_id=$aws_access_key_id;aws_secret_access_key=$aws_secret_access_key' DELIMITER $delim $rs_input_file_format $rs_copy_params;"
	executeStatement "$loadRedshiftStatement"
} # End of loadRedshift



# Parameters (primary keys as well)
function upsertRedshift 
{
	# First create a staging table and copy the data
	local main_rs_table="$rsSchema.$redshiftTable" 
	local staging_suffix=$RANDOM
	local staging_rs_table="$rsSchema.${redshiftTable}_$staging_suffix"
	createLoadStaging $staging_rs_table

	# Delete all data in the main tables with that is in staging  (where primary keys are in it) 
    local primaryKeys=`echo $1 | tr , ' '` # Replace ',' with ' ' so in order to loop through keys
    local keyCompare=""
    for i in $primaryKeys; do
    	keyCompare="$keyCompare and $main_rs_table.$i = $staging_rs_table.$i" 
    done
    keyCompare=${keyCompare:5}  # Remove the first "and" of keyCompare

	local deleteStatement="delete from $main_rs_table using $staging_rs_table where  $keyCompare ;"
	executeStatement "$deleteStatement"

	# Load everyting from the staging table into the main redshift table
	local insertStatement="insert into $main_rs_table select * from $staging_rs_table;"
	executeStatement "$insertStatement"

	local dropStagingStatement="drop table $staging_rs_table;" 	# Delete staging table
	executeStatement "$dropStagingStatement"
} # End of upsertRedshift




## 
# Loads all data to a staging table, drops main table and renames staging to main table
function loadTruncateRedshift
{	
	# First create a staging table and copy the data
	local main_rs_table="$rsSchema.$redshiftTable" 
	local staging_suffix=$RANDOM
	local staging_rs_table="$rsSchema.${redshiftTable}_$staging_suffix"
	createLoadStaging $staging_rs_table

	# Delete main table
	dropMainStatement="drop table $main_rs_table;"
	executeStatement "$dropMainStatement"

	# Rename staging table to main
	renameStagingTable="alter table $staging_rs_table rename to $redshiftTable"
	executeStatement "$renameStagingTable"
}


# Used for bulk delete before copy command.
# * param1: delete column (click_date)
# * param2: delete column value (2015)
#
function bulkDeleteAppendFromRedshift
{
	local deleteColumn="$1"
	local deleteValue="$2"

	# First create a staging table and copy the data
	local main_rs_table="$rsSchema.$redshiftTable" 
	local staging_suffix=$RANDOM
	local staging_rs_table="$rsSchema.${redshiftTable}_$staging_suffix"
	createLoadStaging $staging_rs_table

	# Delete all rows with delete column value
	local main_rs_table="$rsSchema.$redshiftTable" 
	local deleteStatement="delete from $main_rs_table where $deleteColumn = '$deleteValue' ;"
	executeStatement "$deleteStatement"

	# Insert the data from staging table
	local insertStatement="insert into $main_rs_table select * from $staging_rs_table;"
	executeStatement "$insertStatement"

	# Delete staging table
	local dropStagingStatement="drop table $staging_rs_table;"
	executeStatement "$dropStagingStatement"
}  # End of bulkDeleteAppendFromRedshift



if [ "$1" == "true" ] ; then
	echo "setting parameters"
	aws_access_key_id=$4  		 # * PARAM_4
    aws_secret_access_key=$5 		# * PARAM_5
    hdfs_source=$6		# * PARAM_6
    s3_bucket=$7	 		# * PARAM_7
    s3_path=$8			# * PARAM_8
    # Required Redshift copy properties below
    db_host=$9  			# * PARAM_9
    db_port=${10}	 			# * PARAM_10
    db_name=${11}			# * PARAM_11
    db_user=${12} 			# * PARAM_12
    db_pass=${13} 			# * PARAM_13
    rsSchema=${14} 			# * PARAM_14
    redshiftTable=${15} 			# * PARAM_15
    copy_files_prefix=${16} 			# * PARAM_16
    delim="'${17}'" 			# * PARAM_17
    rs_input_file_format=${18} 			# * PARAM_18
    rs_copy_params=${19}    			# * PARAM_19

    # OPTIONAL PARAMETERS HERE (Depending on the script action )
    # Upsert action parameter
    rsUpsertPrimaryKeys=${20}   # "key1,key2,key3"  comma separated  	# * OPTIONAL PARAM_20
    # BulkDeleteAppend action
    rdDeleteAppendColumn=${20}  			# * OPTIONAL PARAM_20
    rsDeleteAppendValue=${21} 			# * OPTIONAL PARAM_21
    copy_s3_path=s3://$s3_bucket$s3_path$copy_files_prefix   # LOGS PREFIX ex:  {bucket-name}/{optional-prefix}{distribution-ID}
fi

if [ "$2" == "load" ] ; then
	copyHDFStoS3 $3
	setupExecute
	loadRedshiftTable
	checkSuccess
elif [ "$2" == "upsert" ] ; then
	copyHDFStoS3 $3
	setupExecute
	upsertRedshift  $rsUpsertPrimaryKeys # Pass the required table primary keys
	checkSuccess
elif [ "$2" == "loadTruncate" ] ; then
	copyHDFStoS3 $3
	setupExecute
    loadTruncateRedshift
    checkSuccess
elif [ "$2" == "bulkDeleteAppend" ] ; then
	copyHDFStoS3 $3
	setupExecute
    bulkDeleteAppendFromRedshift $rdDeleteAppendColumn $rsDeleteAppendValue
    checkSuccess

elif [ "$2" == "copyHDFStoS3" ] ; then
	copyHDFStoS3 $3  

else
    echo "Please specify at least the following three parameters: 'setProperties', 'scriptAction' and 'copyHDFStoS3'"
    echo "The scriptActions are as follow: load, upsert, loadTruncate, bulkDeleteAppend, copyHDFStoS3"
    echo "Refer to script description for detailed usage instructions"
fi
