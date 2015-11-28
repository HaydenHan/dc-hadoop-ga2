#!/bin/sh

ALARM_BASE_URL="http://192.168.1.162:9090/alarm/mr"

HADOOP_CMD="/home/hadoop2/hadoop-2.5.0/bin/hadoop"
SOURCE_HDFS="hftp://dcnamenode1:50070"
TARGET_HDFS="hdfs://decluster"

DATE_STR=`date +%Y/%m/%d`
#rolling info dirs
ROLL_BASE_DIR="/data/digitcube/rolling_day/${DATE_STR}"
ROLL_DONE_FILE="${SOURCE_HDFS}${ROLL_BASE_DIR}/00/done"
ROLL_SRC_FILES="${SOURCE_HDFS}${ROLL_BASE_DIR}/00/output/*USERROLLING_DAY"
ROLL_TARGET_DONE="${TARGET_HDFS}${ROLL_BASE_DIR}/00/done"
ROLL_TARGET_DIR="${TARGET_HDFS}${ROLL_BASE_DIR}/00/output"

#ext info dirs
EXT_ROLL_BASE_DIR="/data/digitcube/extinfo_rolling_day/${DATE_STR}"
EXT_ROLL_DONE_FILE="${SOURCE_HDFS}${EXT_ROLL_BASE_DIR}/00/done"
EXT_ROLL_SRC_FILES="${SOURCE_HDFS}${EXT_ROLL_BASE_DIR}/00/output/*EXT_INFO_ROLL_DAY"
EXT_ROLL_TARGET_DONE="${TARGET_HDFS}${EXT_ROLL_BASE_DIR}/00/done"
EXT_ROLL_TARGET_DIR="${TARGET_HDFS}${EXT_ROLL_BASE_DIR}/00/output"

#data wrap and separate dirs
SEPARATE_DONE_FILE="/data/spark/rollingInfo_day/${DATE_STR}/00/done"
SEPARATE_RESULT_DIR="/data/spark/rollingInfo_day/${DATE_STR}/00/output"
SEPARATE_MR_JAR="/home/hadoop2/tools/dc-hadoop2.jar"
SEPARATE_DEIVER="com.dataeye.hadoop.mapreduce.RollingDataWrapDriver"

#check if UserInfoRollingDay is finish
COUNT=0
while true
do
	$HADOOP_CMD fs -ls $ROLL_DONE_FILE
	if [ $? -eq 0 ]; then
		break;
	fi

	COUNT=`expr $COUNT + 1`
	if [ $COUNT -gt 30 ]; then
		curl "$ALARM_BASE_URL?ip=192.168.1.164&title=DistCopyDataFailed&message=UserInfoRollingDay job is not finish yet."
		exit 1
	fi

	#sleep 30s
	sleep 60
done

#check if UserExtInfoRollingDay is finish
COUNT=0
while true
do
	$HADOOP_CMD fs -ls $EXT_ROLL_DONE_FILE
	if [ $? -eq 0 ]; then
		break;
	fi

	COUNT=`expr $COUNT + 1`
	if [ $COUNT -gt 30 ]; then
		curl "$ALARM_BASE_URL?ip=192.168.1.164&title=DistCopyDataFailed&message=UserExtInfoRollingDay job is not finish yet."
		exit 1
	fi

	#sleep 30s
	sleep 60
done


#copy user info data from hadoop1 to hadoop2
$HADOOP_CMD distcp -update -skipcrccheck ${ROLL_SRC_FILES} ${ROLL_TARGET_DIR}
if [ $? -ne 0 ]; then
	curl "$ALARM_BASE_URL?ip=192.168.1.164&title=DistCopyDataFailed&message=Copy user info data from hadoop1 to hadoop2 failed."
	exit 1
fi
#mark copy done
$HADOOP_CMD fs -touchz ${ROLL_TARGET_DONE}

#copy ext info data from hadoop1 to hadoop2
$HADOOP_CMD distcp -update -skipcrccheck ${EXT_ROLL_SRC_FILES} ${EXT_ROLL_TARGET_DIR}
if [ $? -ne 0 ]; then
	curl "$ALARM_BASE_URL?ip=192.168.1.164&title=DistCopyDataFailed&message=Copy ext info data from hadoop1 to hadoop2 failed."
	exit 1
fi
#mark copy done
$HADOOP_CMD fs -touchz ${EXT_ROLL_TARGET_DONE}


#clear the output dir and start APP data separate MR job
$HADOOP_CMD fs -rm -r ${SEPARATE_RESULT_DIR}
$HADOOP_CMD jar ${SEPARATE_MR_JAR} ${SEPARATE_DEIVER} ${ROLL_TARGET_DIR} ${EXT_ROLL_TARGET_DIR} ${SEPARATE_RESULT_DIR}
if [ $? -ne 0 ]; then
	curl "$ALARM_BASE_URL?ip=192.168.1.164&title=AppDataSeparateFailed&message=App data separate MR job failed."
	exit 1
fi
#mark app data separate done
$HADOOP_CMD fs -touchz ${SEPARATE_DONE_FILE}
#set +x
