SOURCE_BASE_PATH="/Task5"

INPUT_HADOOP_DIR="/Task5/input"
OUTPUT_HADOOP_DIR="/Task5/output"

HADOOP_STREAMING_PATH="${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"

hdfs dfs -test -d ${INPUT_HADOOP_DIR}
if [ $? -eq 0 ];
  then
    echo "Remove ${INPUT_HADOOP_DIR}"
    hdfs dfs -rm -r ${INPUT_HADOOP_DIR}
fi

hdfs dfs -test -d ${OUTPUT_HADOOP_DIR}
if [ $? -eq 0 ];
  then
    echo "Remove ${OUTPUT_HADOOP_DIR}"
    hdfs dfs -rm -r ${OUTPUT_HADOOP_DIR}
fi

test -d ${SOURCE_BASE_PATH}/data/output
if [ $? -eq 0 ];
  then
    echo "Remove ${SOURCE_BASE_PATH}/data/output"
    rm -rf ${SOURCE_BASE_PATH}/data/output
fi

hdfs dfs -mkdir -p ${INPUT_HADOOP_DIR}
hdfs dfs -copyFromLocal ${SOURCE_BASE_PATH}/data/input ${INPUT_HADOOP_DIR}

chmod 0777 ${SOURCE_BASE_PATH}/src/mapper_1.py
chmod 0777 ${SOURCE_BASE_PATH}/src/reducer_1.py

chmod 0777 ${SOURCE_BASE_PATH}/src/mapper_2.py
chmod 0777 ${SOURCE_BASE_PATH}/src/reducer_2.py

chmod 0777 ${SOURCE_BASE_PATH}/src/mapper_3.py
chmod 0777 ${SOURCE_BASE_PATH}/src/reducer_3.py

chmod 0777 ${SOURCE_BASE_PATH}/src/mapper_4.py
chmod 0777 ${SOURCE_BASE_PATH}/src/reducer_4.py

hadoop_streaming_arguments_stage_1="\
  -D mapred.text.key.partitioner.options=-k1,1 \
  -D mapred.text.key.comparator.options=-k1,1 \
  -D mapred.reduce.tasks=3 \
  -files ${SOURCE_BASE_PATH}/src  \
  -mapper src/mapper_1.py -reducer src/reducer_1.py \
  -input ${INPUT_HADOOP_DIR}/* -output ${OUTPUT_HADOOP_DIR}/stage_1 \
  -jobconf stream.num.map.output.key.fields=1 \
"

echo "Run stage 1 streaming with arguments: \n${hadoop_streaming_arguments_stage_1}"
hadoop jar ${HADOOP_STREAMING_PATH} ${hadoop_streaming_arguments_stage_1}

hadoop_streaming_arguments_stage_2="\
  -D mapred.text.key.partitioner.options=-k1n \
  -D mapred.text.key.comparator.options=-k1n,2n \
  -D mapred.reduce.tasks=3 \
  -files ${SOURCE_BASE_PATH}/src  \
  -mapper src/mapper_2.py -reducer src/reducer_2.py \
  -input ${OUTPUT_HADOOP_DIR}/stage_1/* -output ${OUTPUT_HADOOP_DIR}/stage_2 \
  -jobconf stream.num.map.output.key.fields=5 \
"

echo "Run stage 2 streaming with arguments: \n${hadoop_streaming_arguments_stage_2}"
hadoop jar ${HADOOP_STREAMING_PATH} ${hadoop_streaming_arguments_stage_2}

hadoop_streaming_arguments_stage_3="\
  -D mapred.reduce.tasks=3 \
  -files ${SOURCE_BASE_PATH}/src  \
  -mapper src/mapper_3.py -reducer src/reducer_3.py \
  -input ${OUTPUT_HADOOP_DIR}/stage_1/* -input ${OUTPUT_HADOOP_DIR}/stage_2/* \
  -output ${OUTPUT_HADOOP_DIR}/stage_3 \
"

echo "Run stage 3 streaming with arguments: \n${hadoop_streaming_arguments_stage_3}"
hadoop jar ${HADOOP_STREAMING_PATH} ${hadoop_streaming_arguments_stage_3}

hadoop_streaming_arguments_stage_4="\
  -D mapred.reduce.tasks=3 \
  -D mapred.text.key.partitioner.options=-k1,1 \
  -D mapred.text.key.comparator.options=-k1,2 \
  -files ${SOURCE_BASE_PATH}/src  \
  -mapper src/mapper_4.py  -reducer src/reducer_4.py\
  -input ${OUTPUT_HADOOP_DIR}/stage_3/* -output ${OUTPUT_HADOOP_DIR}/final \
"

echo "Run stage 4 streaming with arguments: \n${hadoop_streaming_arguments_stage_4}"
hadoop jar ${HADOOP_STREAMING_PATH} ${hadoop_streaming_arguments_stage_4}

hdfs dfs -copyToLocal ${OUTPUT_HADOOP_DIR} ${SOURCE_BASE_PATH}/data

hdfs dfs -rm -r ${INPUT_HADOOP_DIR}
hdfs dfs -rm -r ${OUTPUT_HADOOP_DIR}
