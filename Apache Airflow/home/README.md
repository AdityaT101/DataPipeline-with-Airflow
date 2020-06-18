This Project revolves around creating the Datapipeline for the following task. The Data originally rests on the S3 buckets, from ther its staged in the Redshift and processed for creating the dimensional model. 

    1. Staging the data on Redshift.
    2. Creating a Dimensional model on the staged schema.
    3. Performing the Data Quality operation on the Dimensional tables.

In addition to this, I have also created follwoing custom operators which are being reused to execute these tasks.

    1. data_quality
    2. load_dimension
    3. load_fact
    4. stage_redshift
    
    
