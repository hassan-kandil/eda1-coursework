# EDA1 Coursework

This repository contains code for setting up and using a cluster to run the Merizo analysis pipeline. Follow the steps below for provisioning a new cluster, running the analysis on existing clusters, downloading output files, and monitoring the cluster.

---

## Table of Contents

- [EDA1 Coursework](#eda1-coursework)
  - [Table of Contents](#table-of-contents)
  - [Provisioning a New Cluster](#provisioning-a-new-cluster)
    - [Prerequisites](#prerequisites)
    - [Steps to Provision the Cluster](#steps-to-provision-the-cluster)
  - [Running Merizo Analysis on Existing Clusters](#running-merizo-analysis-on-existing-clusters)
    - [Steps](#steps)
  - [Downloading Analysis Output Files](#downloading-analysis-output-files)
  - [Cluster Monitoring](#cluster-monitoring)

---

## Provisioning a New Cluster

Follow these instructions to create a new cluster, install required dependencies, configure the environment, and run a Spark job for Merizo analysis.

### Prerequisites
Ensure the following are installed and configured:
1. **Terraform**
2. **Ansible**
3. Update your SSH config file to include:
   ```plaintext
   StrictHostKeyChecking accept-new
   ```

### Steps to Provision the Cluster
1. Initialize Terraform:
   ```bash
   terraform init
   ```
2. Apply the Terraform configuration:
   ```bash
   terraform apply
   ```
3. Set up the cluster using Ansible:
   ```bash
   ansible-playbook -i generate_inventory.py full_setup.yaml --private-key=/path/to/lecturer_private_key
   ```

---

## Running Merizo Analysis on Existing Clusters

Use the following steps to run the Merizo analysis pipeline on an existing cluster for the **E. coli** and **human** datasets.

### Steps
1. **SSH into the host node** using the lecturer private key:
   ```bash
   ssh -i /path/to/lecturer_private_key -J condenser-proxy almalinux@10.134.12.130
   ```
2. **(Optional)** Clean up previous outputs stored on HDFS in the following directories:
   - `/analysis_outputs/`
   - `/summary_outputs/`
3. Navigate to /home/almalinux/merizo_pipeline directory
   ```bash
   cd /home/almalinux/merizo_pipeline
   ```
4. Submit the Spark job:
   ```bash
   spark-submit --deploy-mode cluster --master yarn pipeline_job.py ecoli human
   ```

---

## Downloading Analysis Output Files

Download the analysis and summary output files using the following commands:

1. **Analysis Outputs**
   Contains all intermediate `.tsv` and `.parsed` files for **E. coli** and **human** datasets:
   ```bash
   curl "https://ucabhhk-nginx.comp0235.condenser.arc.ucl.ac.uk/webhdfs/v1/analysis_outputs.tar.gz?op=OPEN&user.name=almalinux" -o analysis_outputs.tar.gz
   ```

2. **Summary Outputs**
   Contains `human_cath_summary.csv`, `ecoli_cath_summary.csv`, and `plDDT_means.csv` summary files:
   ```bash
   curl "https://ucabhhk-nginx.comp0235.condenser.arc.ucl.ac.uk/webhdfs/v1/summary_outputs.tar.gz?op=OPEN&user.name=almalinux" -o summary_outputs.tar.gz
   ```

---

## Cluster Monitoring

Access the following dashboards to monitor the cluster's performance:

1. **HDFS Dashboard:** [https://ucabhhk-hadoop.comp0235.condenser.arc.ucl.ac.uk/](https://ucabhhk-hadoop.comp0235.condenser.arc.ucl.ac.uk/)
2. **YARN Dashboard:** [https://ucabhhk-yarn.comp0235.condenser.arc.ucl.ac.uk/](https://ucabhhk-yarn.comp0235.condenser.arc.ucl.ac.uk/)
3. **Prometheus Dashboard:** [https://ucabhhk-prometheus.comp0235.condenser.arc.ucl.ac.uk/](https://ucabhhk-prometheus.comp0235.condenser.arc.ucl.ac.uk/)
4. **Grafana Dashboard:** [https://ucabhhk-grafana.comp0235.condenser.arc.ucl.ac.uk/](https://ucabhhk-grafana.comp0235.condenser.arc.ucl.ac.uk/)

