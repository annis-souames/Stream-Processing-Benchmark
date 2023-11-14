/home/annis/spark/bin/spark-submit \
  --name "Product Revenues per 3m"\
  --conf "spark.driver.extraJavaOptions=-javaagent:../jmx_prometheus_javaagent-0.18.0.jar=12345:../spark_jmx.yaml" \
  --class org.annis.energy.stream.Main \
  --master local[1] \
  /home/annis/Documents/projects/SparkProcessor/out/artifacts/SparkProcessor_jar/SparkProcessor.jar
