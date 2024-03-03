from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('uefa_analysis').getOrCreate()
uefa=spark.read.csv('hdfs://localhost:9000/spark_project/UEFA_Champions_League_2004-2021.csv',header=True,inferSchema=True)
# uefa.show()
# to print column names
# print('column names are :')
# for i in uefa.columns:
#     print(i)
# #  to print no. of columns
# print(' no. of columns are:',len(uefa.columns))
# # to print no. of rows
# print('no. of rows are :',uefa.count())

# Analysis 1 : draw a graph of away team and home team goal scoring in each year of quaterfinal,semifinal and final. (plot it is 2 graphs)


from pyspark.sql import functions as f
newuefa=uefa.withColumn("date",f.from_unixtime(f.unix_timestamp(uefa.date),"yyyy-MM-dd"))
# newuefa.show()
# newuefa.printSchema()

def year(data):
    if data!='':
        out=data.split('-')
        return out[0]
from pyspark.sql.functions import udf
myfn=udf(year)
newuefa1=newuefa.withColumn('year',myfn(newuefa['date']))
# newuefa1.show()
#
# +---+-------------------+---------+-----------------+---------+---------+-----+----------+----+
# |_c0|           homeTeam|homeScore|         awayteam|awayscore|    round|group|      date|year|
# +---+-------------------+---------+-----------------+---------+---------+-----+----------+----+
# |  0|        Club Brugge|        1|         Juventus|        2|round : 1|    A|2005-09-14|2005|
# |  1|      SK Rapid Wien|        0|   Bayern München|        1|round : 1|    A|2005-09-14|2005|
# |  2|            Arsenal|        2|          FC Thun|        1|round : 1|    B|2005-09-14|2005|
# |  3|       Sparta Praha|        1|             Ajax|        1|round : 1|    B|2005-09-14|2005|
# |  4|            Udinese|        3|    Panathinaikos|        0|round : 1|    C|2005-09-14|2005|
# |  5|      Werder Bremen|        0|        Barcelona|        2|round : 1|    C|2005-09-14|2005|
# |  6|         SL Benfica|        1|        Lille OSC|        0|round : 1|    D|2005-09-14|2005|
# |  7|         Villarreal|        0|Manchester United|        0|round : 1|    D|2005-09-14|2005|
# |  8|              Milan|        3|       Fenerbahçe|        1|round : 1|    E|2005-09-13|2005|
# |  9|      PSV Eindhoven|        1|    FC Schalke 04|        0|round : 1|    E|2005-09-13|2005|
# | 10|         Olympiacos|        1|     Rosenborg BK|        3|round : 1|    F|2005-09-13|2005|
# | 11| Olympique Lyonnais|        3|      Real Madrid|        0|round : 1|    F|2005-09-13|2005|
# | 12|            Chelsea|        1|   RSC Anderlecht|        0|round : 1|    G|2005-09-13|2005|
# | 13|Real Betis Balompié|        1|        Liverpool|        2|round : 1|    G|2005-09-13|2005|
# | 14|  FC Petrzalka 1898|        0|            Inter|        1|round : 1|    H|2005-09-13|2005|
# | 15|            Rangers|        3|         FC Porto|        2|round : 1|    H|2005-09-13|2005|
# | 16|     Bayern München|        1|      Club Brugge|        0|round : 2|    A|2005-09-27|2005|
# | 17|           Juventus|        3|    SK Rapid Wien|        0|round : 2|    A|2005-09-27|2005|
# | 18|               Ajax|        1|          Arsenal|        2|round : 2|    B|2005-09-27|2005|
# | 19|            FC Thun|        1|     Sparta Praha|        0|round : 2|    B|2005-09-27|2005|
# +---+-------------------+---------+-----------------+---------+---------+-----+----------+----+
# only showing top 20 rows

#
flt_out=newuefa1.filter((newuefa1['round']=='round : quarterfinals')
         |(newuefa1['round']=='round : semifinals')
         |(newuefa1['round']=='round : final'))
# flt_out.show()
new=flt_out.select('homeScore','awayScore','round','year')
# new.show(n=10)
# +---------+---------+--------------------+----+
# |homeScore|awayScore|               round|year|
# +---------+---------+--------------------+----+
# |        2|        0|round : quarterfi...|2006|
# |        0|        0|round : quarterfi...|2006|
# |        2|        1|round : quarterfi...|2006|
# |        0|        0|round : quarterfi...|2006|
# |        3|        1|round : quarterfi...|2006|
# |        1|        0|round : quarterfi...|2006|
# |        2|        0|round : quarterfi...|2006|
# |        0|        0|round : quarterfi...|2006|
# |        0|        1|  round : semifinals|2006|
# |        1|        0|  round : semifinals|2006|
# +---------+---------+--------------------+----+
# only showing top 10 rows

def score(data):
    out=data.split('(')
    return out[0]
newfn=udf(score)
new1=new.withColumn('home_score',newfn(new['homeScore']))
new2=new1.withColumn('away_score',newfn(new['awayScore']))
# new2.show(n=50)
final=new2.drop('homeScore','awayScore')
# final.show()
# +--------------------+----+----------+----------+
# |               round|year|home_score|away_score|
# +--------------------+----+----------+----------+
# |round : quarterfi...|2006|         2|         0|
# |round : quarterfi...|2006|         0|         0|
# |round : quarterfi...|2006|         2|         1|
# |round : quarterfi...|2006|         0|         0|
# |round : quarterfi...|2006|         3|         1|
# |round : quarterfi...|2006|         1|         0|
# |round : quarterfi...|2006|         2|         0|
# |round : quarterfi...|2006|         0|         0|
# |  round : semifinals|2006|         0|         1|
# |  round : semifinals|2006|         1|         0|
# |  round : semifinals|2006|         0|         0|
# |  round : semifinals|2006|         0|         0|
# |       round : final|2006|         2|         1|
# |round : quarterfi...|2007|         2|         2|
# |round : quarterfi...|2007|         0|         3|
# |round : quarterfi...|2007|         1|         1|
# |round : quarterfi...|2007|         2|         1|
# |round : quarterfi...|2007|         7|         1|
# |round : quarterfi...|2007|         1|         2|
# |round : quarterfi...|2007|         0|         2|
# +--------------------+----+----------+----------+
# only showing top 20 rows


# from pyspark.sql.types import IntegerType
# newdf=final.withColumn('hmscore',final['home_score'].cast(IntegerType()))
# finaldf=newdf.withColumn('awscore',newdf['away_score'].cast(IntegerType()))
# finaldf=finaldf.drop('home_score','away_score')
# # finaldf.show()
# # finaldf.printSchema()
#
#
# import pyspark.sql.functions as f
# grp=finaldf.groupBy('year').agg(f.sum('hmscore').alias('totalhomegoals'),f.sum('awscore').alias('totalawaygoals'))
# #
# # grp.show()
# grp=grp.orderBy('year')
# import pandas as pd
# df=grp.toPandas()
#
# import matplotlib.pyplot as plt
# plt.plot(df['year'],df['totalhomegoals'])
# plt.plot(df['year'],df['totalawaygoals'])
# plt.show()

# analysis 2 - teams that most appeared in quarterfinal,semifinal, and final
#
qf=uefa.filter(uefa['round'] =='round : quarterfinals')
sf=uefa.filter(uefa['round'] =='round : semifinals')
fi=uefa.filter(uefa['round'] =='round : final')

qf=qf.select('_c0','homeTeam','round','date')
sf=sf.select('_c0','homeTeam','round','date')
fi=fi.select('_c0','homeTeam','round','date')

# qf.show()
# sf.show()
# fi.show()

import pyspark.sql.functions as f
li=[qf,sf,fi]
for i in li:
    out1 = i.groupBy('homeTeam').agg(f.count('_c0').alias('no_of_participations'))
    out2 = out1.orderBy('no_of_participations', ascending=False)
    out2.show()
    # out2.head(1)
    maxvalue=out2.select(f.max(out2.no_of_participations))
    print(maxvalue.collect()[0])
    print('...................................')

# +-------------------+--------------------+
# |           homeTeam|no_of_participations|
# +-------------------+--------------------+
# |          Barcelona|                  14|
# |     Bayern München|                  11|
# |        Real Madrid|                   9|
# |  Manchester United|                   7|
# |            Chelsea|                   7|
# |           Juventus|                   6|
# |          Liverpool|                   6|
# |    Manchester City|                   5|
# |Paris Saint-Germain|                   5|
# |  Borussia Dortmund|                   4|
# |            Arsenal|                   4|
# |    Atlético Madrid|                   4|
# |           FC Porto|                   4|
# |               Roma|                   3|
# |         SL Benfica|                   3|
# |              Milan|                   3|
# |              Inter|                   3|
# |          Tottenham|                   2|
# |      FC Schalke 04|                   2|
# | Olympique Lyonnais|                   2|
# +-------------------+--------------------+
# only showing top 20 rows
# Row(max(no_of_participations)=14)
# ...................................

# +-------------------+--------------------+
# |           homeTeam|no_of_participations|
# +-------------------+--------------------+
# |        Real Madrid|                   9|
# |          Barcelona|                   9|
# |     Bayern München|                   7|
# |            Chelsea|                   6|
# |  Manchester United|                   4|
# |          Liverpool|                   4|
# |    Atlético Madrid|                   3|
# |           Juventus|                   2|
# | Olympique Lyonnais|                   2|
# |              Milan|                   2|
# |            Arsenal|                   2|
# |    Manchester City|                   2|
# |          Tottenham|                   1|
# |         RB Leipzig|                   1|
# |      FC Schalke 04|                   1|
# |         Villarreal|                   1|
# |              Inter|                   1|
# |               Ajax|                   1|
# |Paris Saint-Germain|                   1|
# |          AS Monaco|                   1|
# +-------------------+--------------------+
# only showing top 20 rows
# Row(max(no_of_participations)=9)
# ...................................

# +-------------------+--------------------+
# |           homeTeam|no_of_participations|
# +-------------------+--------------------+
# |        Real Madrid|                   3|
# |          Barcelona|                   3|
# |     Bayern München|                   2|
# |           Juventus|                   2|
# |          Tottenham|                   1|
# |    Manchester City|                   1|
# |  Manchester United|                   1|
# |              Milan|                   1|
# |  Borussia Dortmund|                   1|
# |Paris Saint-Germain|                   1|
# +-------------------+--------------------+
# Row(max(no_of_participations)=3)
# ...................................


