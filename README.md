# Movielens project

## Instructions to run jar file

* In order to run the jar file, you need to provide one argument:
    - Path to the folder where the movielens files are located, please include a backslash at the end "\"

* Example:
    - ./bin/spark-submit --master spark://BLV-U-OS:7077 
    	/home/blarico/Dev/client_project_improved/target/scala-2.12/client_project_improved_2.12-0.1.jar 
   	/home/blarico/Dev/client_project_improved/src/main/resources/

* Template:
    - spark-submit --master <Spark_master_URL>
     <JAR_FILE_PATH>
     <MOVIE_LENS_FOLDER>
