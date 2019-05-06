# Ripple

Ripple is a serverless computing framework designed to make complex applications on AWS Lambda.
Ripple allows the user to chain lambda functions using S3 triggers 

## Setup
Ripple works by chaining a series of Lambda functions together.
We'll walk through one simple Ripple example.

### Copy File
The first example will simply copy the input file that triggered the pipeline.
To start, run `simple.py` in the examples folder.
This file sets up a pipeline that calls an application called `echo`.
`echo.py` can be found in the `applications` folder.
It takes an input file that we specify to be new line delimited and writes it out to a new file.
When ``simple.py`` is run, the pipeline is created and uploadedto Lambda.
The pipeline is also written to a JSON file called `simple.json` to the json folder.
The JSON file will look like the following.

```
{
  "bucket": "maccoss-tide",
  "log": "maccoss-log",
  "timeout": 600,
  "functions": {
    "echo": {
      "application": "echo",
      "file": "application",
      "input_format": "new_line",
      "memory_size": 1024,
      "output_format": null,
      "formats": [
        "new_line"
      ],
      "imports": []
    }
  },
  "pipeline": [
    {
      "name": "echo"
    }
  ],
  "region": "us-west-2",
  "role": "service-role/lambdaFullAccessRole"
}
```

The `functions` part is the set of all Lambda functions needed by the pipeline.
A function can be used multiple times in the pipeline.
This section specifies parameters that are applicable to every instance of the function, such as the amount of allocated memory or the file format.

The `pipeline` part specifies the order of execution of the Lambda functions.
The parameters in the section are for only instances that occur in this stage of the pipeline.
For example, you may want to call the same combine function twice in the pipeline, but only have one of them sort the output.

The files written will have the format `<prefix>/<timestamp>-<nonce>/<bin_id>-<num_bins>/<file_id>-<execute>-<num_files>.<ext>.

The prefix indicates the stage of the of the pipeline that wrote the file.
In the above example, the input that triggers the pipeline will have the prefix "0" and the output from the echo function will have the prefix "1".
The timestamp indicates when a given run was instantiated and the nonce is an identifier for the run.
The bin ID specifies which bin the file is in. The bins are used to sort and combine subsets of data.
Each file in a bin is given a number between 1 and the number of files in the bin.
The execute value is used if we need to force a function to re-execute or to tell a function not to execute (this is used for deadline scheduling).

## Trigger Pipeline
To upload a file to a Lambda pipeline, run:
python3 upload.py --destination_bucket_name <bucket-used-for-application> --key <name-of-file-to-upload> [--source_bucket_name <s3-bucket-input-file-is-located-in>
A user can either upload a file from their computer or from S3, however using the upload script is easier, as it handles formatting the file name.

## Functions
### Application
The application function allows a user to execute arbitrary code on the input file
##### Arguments
* application: The name of the application file in the `applications` directory to use.

### Combine
The combine function takes all files in a bin and concats the files into one file.
##### Arguments
* batch_size: Number of files to combine.
* chunk_size: Number of bytes to load at once.
* format: Type of file to be split (.mzML, .txt, .csv)
* identifier: Property to sort file values by.
* sort: True if the file values should be sorted.

### Initiate
The initiate function triggers a new lambda based on a prior step.
This is useful if there's a need to use data written by a Lambda other than the parent Lambda.
##### Arguments
* input_key_prefix: Prefix for the file we want to use.
* output_function: The lambda function to call for each bucket file.

### Map
The map function maps the input file to each file contained in a bucket.
##### Arguments
* bucket_key_value: The parameter the bucket file should represent in the next function.
* directories: True if the map function should send the directories in the map bucket, not the files.
* format: Type of file to be split (.mzML, .txt, .csv)
* input_key_value: The parameter the input file should represent in the next function.
* map_bucket: Bucket containing list of files to map.
* output_function: The lambda function to call for each bucket file.
* ranges: True if the input file represents pivots for sorting.

### Match
Give a set of keys, match returns the key with the highest score
##### Arguments
* chunk_size: Number of bytes to load at once.
* format: Type of file to be split (.mzML, .txt, .csv)
* find: What to look for. Currently only supports highest sum.
* identifier: Property to sort file values by.

### Pivot
Given a number of bins and an input file / file chunk, this function finds `num_bins` equally spaced pivots.
This is used for sorting input bins.
##### Arguments
* chunk_size: Number of bytes to load at once.
* identifier: Property to sort file values by.

### Sort
The sort function partial sorts the input by writing values to bins based on their sort value.
This function requires bin boundaries to be specified. This can be done using the `pivot_file` function.
##### Arguments
* chunk_size: Number of bytes to load at once.
* identifier: Property to sort file values by.

### Split
The split function sends offsets for a key to lambda functions so the file can be analyzed in parallel.
##### Arguments
* chunk_size: Number of bytes to load at once.
* format: Type of file to be split (.mzML, .txt, .csv)
* output_function: The lambda unction to call with each chunk.
* ranges: True if the input file represents pivots for sorting.

### Top
The top function extracts the items with the highest identifier value.
##### Arguments
* chunk_size: Number of bytes to load at once.
* format: Type of file to be split (.mzML, .txt, .csv
* identifier: Property to sort file values by.
* number: Number of values to return
