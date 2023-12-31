# Sparta Data 249 ⚙ Final Project 


For our final project, we've created an ETL pipeline to ingest, transform, and input fictional Sparta Global data into a database which provides a single person view.



                                                    ~⚙~⚙~

![ETL Gif](./images/etl-pipeline.gif)

                                                    ~⚙~⚙~

# Contents

1. [Methodology](#methodology)
2. [Data Transformations](#transformations)
3. [How to Install](#howto)
4. [Credits](#credits)

<a name="methodology"></a>
#  Methodology

We followed Agile methodology and a Scrum framework to carry out this project, using  a Trello board to prioritise work and map out user stories.   
    

![Screenshot of our Trello Board](./images/trello-board.png)

## <a href= "https://trello.com/b/r6ubxE2s/final-project">Link to our Trello Workspace</a>

                                                    ⚙⚙⚙


The data are loaded into a SQL database which is normalised to third normal form. Creating an Entity Relation Diagram was an important preliminary step in the pipeline process, which enabled us to plan our data manipulation and write our scripts. 

![Screenshot of our ERD](./images/erd.png)


## <a href= "https://drive.google.com/file/d/1ooZ4fmxefSlmGnYa4AS6cw7-YV5a2VI2/view?usp=sharing">Link to our Full ERD Diagram</a>

                                                    ~⚙~⚙~

<a name="transformations"></a>
## Tranformations

Before we loaded our data into the SQL database, we needed to transform quite a bit of it. In particular data types had to be assigned to ensure that the data was in the correct type, since in the original data frames, all of the columns were stored as object data type:

### ⚙ Data Type Transformations ⚙

-   Pyschometric & presentation scores -> Integers
- Dates -> Datetime
- JSON fields -> Bit type

There were some other data transformations which were necessary to ensure that our data complied with normalisation, was atomic & able to be inputted smoothly into our database strcture as set out in the ERD.

### ⚙ Data Manipulations ⚙

- Names split into first name & surname
- House number & street split into two 
- Weaknesses & strengths split into two 
- Tech scores in dictionary split into separate columns

We also changed all of the names into lower case so as to ensure uniformity and matching. 

                                                    ~⚙~⚙~


<a name="howto"></a>
## How to Run and Install

In order to run the pipeline, it is first necessary to set up the database in a SQL server such as Azure Data Studio or Microsoft SQL Server. The script to create the database is provided in the config file. This should be run, and the database structure set up before the pipeline script can be deployed. 

![Sparta](./images/sparta-global.webp)

<a name="credits"></a>
# Credits


### SCRUM Master: 
<a href= "https://www.github.com/MW200410">Martin Wormwell @MW200410</a> 



### Product Owner: 
<a href= "https://www.github.com/taslimahossain">Taslima Hossain @taslimahossain</a> 

### Git Authority

<a href= "https://www.github.com/Yuvraj-26">Yuvraj Mahida @Yuvraj-26</a>



### Data Engineering Team

- <a href= "https://www.github.com/ruthChanarin">Ruth Chanarin @ruthChanarin </a>

- <a href= "https://www.github.com/tommyainsworth">Tommy Ainsworth @tommyainsworth </a>

- <a href= "https://www.github.com/Kill-Hxps">Killian Hughes @Kill-Hxps </a>

- <a href= "https://www.github.com/wilkiesophie">Sophie Wilkie @wilkiesophie </a>

- <a href= "https://www.github.com/IsakGrimsson">Isak Grimsson @IsakGrimsson </a>

- <a href= "https://www.github.com/JCav23">Jack Cavanagh @JCav23 </a>

- <a href= "https://www.github.com/LaurenG123">Lauren Gorst @LaurenG123 </a>

- <a href= "https://www.github.com/Karisjr">Karis Reimers @Karisjr </a>

- <a href= "https://www.github.com/andyc2901">Andrew Carver @andyc2901 </a>


Also, big credit & thanks to Tommy & Paula, our wonderful and ever so patient trainers, for giving us the skills to put this project together, and empowering us in our data engineering journeys. 

