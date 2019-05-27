# Projet bigdata bank

First step make all bash scripts executable with this command:
> chmod +x hadoop_*

Second step execute this bash script to create input folder and upload the csv file into hadoop file system
> ./hadoop_input_preparation

Next execute this script to run all hadoop jobs
> ./hadoop_execute_mapred

Finally to show results execute:
> ./hadoop_print_output

Optionally you can reset all changes:
> ./hadoop_clear
