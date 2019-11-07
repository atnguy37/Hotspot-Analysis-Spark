# Analysis of Geospatial Operations on Distributed Data Platform
Applying spatial statistics to spatio-temporal big data in order to identify statistically significant spatial hot spots using Apache Spark and Scala.
Load GeoSpark jar into Apache Spark Scala shell

## PHASE 1:
Write two User Defined Functions ST_Contains and ST_Within in SparkSQL and use them to do four spatial queries:

1. Range query: Use ST_Contains. Given a query rectangle R and a set of points P, find all the points within R.

2. Range join query: Use ST_Contains. Given a set of Rectangles R and a set of Points S, find all (Point, Rectangle) pairs        such that the point is within the rectangle.

3. Distance query: Use ST_Within. Given a point location P and distance D in km, find all points that lie within a distance D    from P

4. Distance join query: Use ST_Within. Given a set of Points S1 and a set of Points S2 and a distance D in km, find all (s1,      s2) pairs such that s1 is within a distance D from s2 (i.e., s1 belongs to S1 and s2 belongs to S2).
