from pyspark.sql import SparkSession
import random
import sys
import os
import shutil

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Monte Carlo Pi Estimation").getOrCreate()
    
    total_samples = 100000
    if len(sys.argv) != 2:
        print("Usage: pi.py <output_path>")
        sys.exit(1)
    
    output_path = sys.argv[1]
    temp_output_path = output_path + "_temp"

    rdd = spark.sparkContext.parallelize(range(total_samples))
    count = (
        rdd.map(lambda _: (random.random() ** 2 + random.random() ** 2) <= 1)
        .filter(lambda x: x)
        .count()
    )
    pi_estimate = 4 * count / total_samples
    result_df = spark.createDataFrame([(total_samples, pi_estimate)], ["num_samples", "estimated_pi"])

    # ✅ CSV를 일단 _temp 폴더에 저장
    result_df.coalesce(1).write.mode("overwrite").csv(temp_output_path, header=True)

    # ✅ 저장된 파일명을 'pi_result.csv'로 변경
    temp_folder = os.path.join(output_path + "_temp")
    final_file_path = os.path.join(output_path, "pi_result.csv")

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    for file in os.listdir(temp_folder):
        if file.startswith("part-") and file.endswith(".csv"):
            shutil.move(os.path.join(temp_folder, file), final_file_path)

    shutil.rmtree(temp_folder)  # 임시 폴더 삭제

    print(f"✅ CSV 저장 완료: {final_file_path}")
    spark.stop()