
#! /bin/sh
#进程名字可修改
PRO_NAME_ARR=('./downloadMongo.py'
            )


while true ; do
    for PRO_NAME in ${PRO_NAME_ARR[@]};do
         #用ps获取$PRO_NAME进程数量
         NUM=`ps aux | grep -w "${PRO_NAME}" | grep -v grep |wc -l`
         echo $PRO_NAME
         echo $NUM
         #少于1，重启进程
         if [ "${NUM}" -lt "1" ];then
             echo "${PRO_NAME} was killed"
             nohup ${PRO_NAME} &
             echo "${PRO_NAME} restart"
        #大于1，杀掉所有进程，重启
       # elif [ "${NUM}" -gt "1" ];then
        #   echo "more than 1 ${PRO_NAME},killall ${PRO_NAME}"
         #    killall -9 $PRO_NAME
          #  ${PRO_NAME} -d
         fi
         #kill僵尸进程
        # NUM_STAT=`ps aux | grep -w ${PRO_NAME} | grep T | grep -v grep | wc -l`
        # if [ "${NUM_STAT}" -gt "0" ];then
         #    killall -9 ${PRO_NAME}
          #   ${PRO_NAME} -d
        #fi
    done
    sleep 60s
done
exit 0

