# multinational-retail-data-centralisation271
Development of a star schema for the data of a multinational company starting from the extraction of data from different sources, cleaning the data, and then uploading to a local database for querying. The structure of the star schema can be seen below.

![star base schema](Images/star.png)

<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#Installation">Installation</a></li>
    <li><a href="#Usage">Usage</a></li>
    <li>
      <a href="#File-Structure">File Structure</a>
      <ul>
        <li>
          <a href="#retail.ipynb">retail.ipynb</a>
          <ul>
            <li><a href="#database_utils.py">database_utils.py</a></li>
            <li><a href="#data_extraction.py">data_extraction.py</a></li>
            <li><a href="#data_cleaning.py">data_cleaning.py</a></li>
          </ul>
        </li>
          <li><a href="#set_primary_keys.sql">set_primary_keys.sql</a></li>
          <li><a href="#set_foreign_keys.sql">set_foreign_keys.sql</a></li>
          <li><a href="#m4-folder">m4 folder</a></li>
      </ul>
    </li>
    <li><a href="#License">License</a></li>
  </ol>
</details>

## Installation
Clone for local access.
```sh
clone https://github.com/bc319IC/multinational-retail-data-centralisation271.git`
```

## Usage
Run the appropraite files in the following section regarding their descriptions.

## File Structure

### retail.ipynb <a id="retail.ipynb"></a>
Run all in retail.ipynb for the data extraction of each table using different methods due to different sources, followed by the cleaning of the data, and uploading to the local postgres database.

#### database_utils.py <a id="database_utils.py"></a>
Contains the class for connecting to the local postgres database.

#### data_extraction.py <a id="data_extraction.py"></a>
Contains the class for the different types of data extraction methods.

#### data_cleaning.py <a id="data_cleaning.py"></a>
Contains the class for cleaning all the dataframes in this project.

### set_primary_keys.sql <a id="set_primary_keys.sql"></a>
Contains the code to set the necessary columns as primary keys.

### set_foreign_keys.sql <a id="set_foreign_keys.sql"></a>
Contains the code to set the necessary columns as foreign keys.

### m4 folder
Contains SQL queries that should be self explanatory by name.


## License Information
This project is licensed under the terms of the MIT license.