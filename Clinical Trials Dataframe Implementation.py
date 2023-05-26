#!/usr/bin/env python
# coding: utf-8

# #ANALYSIS OF CLINICAL TRIAL DATA BY PAMELA YUSUF @00646995

# ##DATAFRAME IMPLEMENTATION

# In[ ]:


#installing necessary visualization software
get_ipython().run_line_magic('pip', 'install bokeh')


# In[ ]:


get_ipython().run_line_magic('pip', 'install wordcloud')


# In[ ]:


# Confirming that files were unzipped and copied successfully
dbutils.fs.ls("FileStore/tables")


# ####Loading the Clinical Trial Data

# In[ ]:


year = 2021


# In[ ]:


# storing the clinial trial data in a variable to aid reusability 
clinical_filepath = "/FileStore/tables/clinicaltrial_"+ str(year) + ".csv"


# In[ ]:


#viewing the first few lines of the file
dbutils.fs.head(clinical_filepath)


# In[ ]:


# Taking a clearer look at the data
for line in dbutils.fs.head(clinical_filepath).splitlines():
    print(line)


# In[ ]:


#reading the clinial trial data into a dataframe and checking to see if it was read in correctly
clinical_df = spark.read.option("delimiter", "|").option("header" , "True").option("inferSchema" ,"True").csv(clinical_filepath)
clinical_df.show(10)


# In[ ]:


clinical_df.printSchema()


# In[ ]:


#Taking a broad look at the clinical trial data
clinical_df.display()


# ##ANALYSING THE DATA

# ###Problem Statement 1 - Obtaining the number of distinct studies in the dataset

# In[ ]:


# Selecting the distinct rows and counting them
no_of_distinct_studies = clinical_df.distinct().count()
no_of_distinct_studies


# ###Problem Statement 2 - Listing all the types of studies in the data set along with their frequencies

# In[ ]:


#Grouping the data by trials, counting frequencies, then sorting from most frequent to least frequent
types_list = clinical_df.groupBy('Type').count().sort('count', ascending=False)
types_list.show(truncate = False)


# ###Problem Statement 3 - Displaying the top 5 conditions with their frequencies

# In[ ]:


#viewing the conditions column
clinical_df.select("Conditions").show(truncate = False)


# In[ ]:


#splitting the conditions column row by row in order for it to be exploded
from pyspark.sql.functions import split
splitDF = clinical_df.withColumn('Conditions', split(clinical_df["Conditions"], ","))
splitDF.show()


# In[ ]:


#exploding the split array
from pyspark.sql.functions import explode
explodedDF = splitDF.withColumn('Conditions', explode(splitDF["Conditions"]))
explodedDF.show()


# In[ ]:


#grouping and counting the exploded conditions (and then displaying from most frequent to least frequent for better view)
explodedDF.groupBy('Conditions').count().sort('count', ascending=False).show(5)


# ###Problem Statement 4 - The 5 most frequent roots

# ####Loading the Mesh Data

# In[ ]:


#viwing the first few lines 
dbutils.fs.head("/FileStore/tables/mesh.csv")


# In[ ]:


#looking at the data more clearly
for line in dbutils.fs.head("/FileStore/tables/mesh.csv").splitlines():
    print(line)


# In[ ]:


# reading the mesh data into a dataframe and checking to see if it was read in correctly
mesh_df = spark.read.option("delimiter", ",").csv( "/FileStore/tables/mesh.csv", header = True , inferSchema = True)
mesh_df.show(10)
mesh_df.printSchema()


# In[ ]:


#extracting the root from the tree using substring to create root column
meshrootDF = mesh_df.withColumn("root", mesh_df.tree.substr(1,3))
meshrootDF.show()


# In[ ]:


#creating a dataframe containing the columns needed from the mesh data for problem analysis
termrootDF = meshrootDF.select("term" , "root")
termrootDF.show()


# In[ ]:


#creating a dataframe containing the columns needed from the clinical trial data for problem analysis
explodedConditions = explodedDF.select('Conditions')
explodedConditions.show()


# In[ ]:


#Joining the two dataframes using inner join
joinedDF = explodedConditions.join(termrootDF,explodedConditions.Conditions == termrootDF.term)
joinedDF.show()


# In[ ]:


#Grouping by the root column, counting and sorting 
frequentroots = joinedDF.groupBy('root').count().sort('count', ascending=False)
frequentroots.show(5)


# ###Problem Statement 5 - Ten most common sponsors that are not pharmacetical companies

# ####Loading the Pharma Data

# In[ ]:


#checking the head of the file 
dbutils.fs.head("/FileStore/tables/pharma.csv")


# In[ ]:


#Taking a look at the first few lines clearly
for line in dbutils.fs.head("/FileStore/tables/pharma.csv").splitlines():
    print(line)


# In[ ]:


# reading the pharma data into a dataframe and checking to see if it was read in correctly
pharma_df = spark.read.option("delimiter", ",").csv( "/FileStore/tables/pharma.csv", header = True , inferSchema = True)
pharma_df.show(10)
pharma_df.printSchema()


# In[ ]:


#Selecting the column needed from pharma data for analysis
ParentCompanyDF = pharma_df.select("Parent_Company")
ParentCompanyDF.show()


# In[ ]:


#Selecting the column needed from clinical trial data for analysis
SponsorsDF = clinical_df.select("Sponsor")
SponsorsDF.show()


# In[ ]:


#Joining the two dataframes using left join
joinedpharmaDF = SponsorsDF.join(ParentCompanyDF,SponsorsDF.Sponsor == ParentCompanyDF.Parent_Company, "left")
joinedpharmaDF.display()


# In[ ]:


#Extracting the rows that have null values in the joined dataframe, grouping and 
nonpharma_Sponsors = joinedpharmaDF.where("Parent_Company is null")
nonpharma_Sponsors.show()


# In[ ]:


#grouping, counting and sorting
common_nonpharma_Sponsors = nonpharma_Sponsors.groupBy('Sponsor').count().sort('count', ascending=False)
common_nonpharma_Sponsors.show(10, truncate = False)


# ###Problem 6 - Number of Completed Studies In A Given Year

# In[ ]:


#Viewing the clinical trial dataframe again
clinical_df.show()


# In[ ]:


#Extracting studies that have been completed
CompletedStudies = clinical_df.select("Completion", "Status")                                .filter(clinical_df.Status == "Completed")
CompletedStudies.show()


# In[ ]:


#Seperating the month and the year into two seperate columns
split_completedstudies = CompletedStudies.withColumn("Month", CompletedStudies.Completion.substr(1,3))                                         .withColumn("Year", CompletedStudies.Completion.substr(5,8))
split_completedstudies.show()


# In[ ]:


#Extracting column needed and filtering for concerned year
CompletedStudies_year = split_completedstudies.select("Month" , "Year")                                              .filter(split_completedstudies.Year == year)
CompletedStudies_year.show()


# In[ ]:


Monthly_CompletedStudies = CompletedStudies_year.groupby('Month').count().sort("count")
Monthly_CompletedStudies.show()


# In[ ]:


from pyspark.sql.functions import col, when

Ordered_Monthly_CompletedStudies = Monthly_CompletedStudies.orderBy(when(col("Month") == "Jan", 1)
           .when(col("Month") == "Feb", 2)
           .when(col("Month") == "Mar", 3)
           .when(col("Month") == "Apr", 4)
           .when(col("Month") == "May", 5)
           .when(col("Month") == "Jun", 6)
           .when(col("Month") == "Jul", 7)
           .when(col("Month") == "Aug", 8)
           .when(col("Month") == "Sep", 9)
           .when(col("Month") == "Oct", 10)
           .when(col("Month") == "Nov", 11)
           .when(col("Month") == "Dec", 12)
           )


# In[ ]:


Ordered_Monthly_CompletedStudies.show()


# ##VISUALIZATIONS

# In[ ]:


#converting the pyspark dataframe to pandas dataframe to aid plotting
import pandas as pd


# ###Using Bokeh

# In[ ]:


Months = list(Ordered_Monthly_CompletedStudies.toPandas()['Month'])


# In[ ]:


Months


# In[ ]:


count = list(Ordered_Monthly_CompletedStudies.toPandas()['count'])


# In[ ]:


count


# In[ ]:


import random
from bokeh.io import output_file, show
from bokeh.plotting import figure
from bokeh.embed import components, file_html
from bokeh.resources import CDN
from bokeh.palettes import Viridis256

output_file("Monthly_completed_studies.html")

p = figure(x_range= Months, height=400, title="Completed Studies each month for year 2021", 
           toolbar_location=None ,tooltips = [("Month",  "@x"), ("Count" , "@top")])

p.vbar(x=Months, top=count, width=0.9,color = random.sample(Viridis256,10), fill_alpha=.75)

p.xgrid.grid_line_color = None
p.y_range.start = 0

html = file_html(p, CDN, "Completed Studies each month for year 2021")
displayHTML(html)


# ###Using Matplotlib

# In[ ]:


import matplotlib.pyplot as plt

Ordered_Monthly_CompletedStudies.toPandas().plot(x="Month", y="count", kind='line',  marker='*', color='red', ms=10
                                                ,title="Number of Completed Studies each month - Year 2021", figsize=(15, 6))

plt.show()


# #FURTHER ANALYSIS

# ##Exploring the top Interventions

# In[ ]:


#viewing the interventions column
clinical_df.select("Interventions").show(truncate = False)


# In[ ]:


#splitting the conditions column row by row in order for it to be exploded
from pyspark.sql.functions import split
splitinterventionsDF = clinical_df.withColumn('Interventions', split(clinical_df["Interventions"], ","))
splitinterventionsDF.show()


# In[ ]:


#exploding the split array
from pyspark.sql.functions import explode
explodedinterventionsDF = splitinterventionsDF.withColumn('Interventions', explode(splitinterventionsDF["Interventions"]))
explodedinterventionsDF.show()


# In[ ]:


completedinterventions = explodedinterventionsDF.filter(clinical_df.Status == 'Completed')


# In[ ]:


interventions_list = list(completedinterventions.toPandas()['Interventions'])


# In[ ]:


interventions_list


# In[ ]:


#converting the list to a dictionary with values and its occurences
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
from collections import Counter
word_could_dict=Counter(interventions_list)
wordcloud = WordCloud(width = 1000, height = 500).generate_from_frequencies(word_could_dict)

plt.figure(figsize=(15,8))
plt.imshow(wordcloud)
plt.axis("off")
plt.show()
plt.savefig('yourfile.png', bbox_inches='tight')
plt.close()


# ## Visualizing the status of clinical trials

# In[ ]:


#Grouping the data by trials, counting frequencies, then sorting from most frequent to least frequent
status_list = clinical_df.groupBy('Status').count().sort('count', ascending=False)
status_list.show(truncate = False)


# In[ ]:


df = status_list.toPandas()


# In[ ]:


df


# In[ ]:


Status = list(status_list.toPandas()['Status'])


# In[ ]:


Status


# In[ ]:


Frequency = list(status_list.toPandas()['count'])


# In[ ]:


Frequency


# In[ ]:


my_dict = dict(zip(Status, Frequency))


# In[ ]:


my_dict


# In[ ]:


from math import pi

import pandas as pd

from bokeh.palettes import Category20c
from bokeh.plotting import figure, show
from bokeh.transform import cumsum


data = pd.Series(my_dict).reset_index(name='value').rename(columns={'index': 'Status'})
data['angle'] = data['value']/data['value'].sum() * 2*pi
data['color'] = Category20c[len(my_dict)]
data['percent'] = data['value'] / sum(my_dict.values()) * 100

p = figure(height=350, title="Pie Chart showing status of clinical trials", toolbar_location=None,
           tools="hover", tooltips=[("Status" ,"@Status") ,("Count" , "@value") , ("Percentage" , "@percent{0.2f} %")], x_range=(-0.5, 1.0))

p.wedge(x=0, y=1, radius=0.4,
        start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
        line_color="white", fill_color='color', legend_field='Status', source=data)

p.axis.axis_label = None
p.axis.visible = False
p.grid.grid_line_color = None

html = file_html(p, CDN, "Status of Clinical Trials")
displayHTML(html)


# In[ ]:




