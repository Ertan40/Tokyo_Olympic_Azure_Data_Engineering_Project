---Number of medals won by each country
SELECT teamcountry, 
   SUM(gold) AS "Total_Gold",
   SUM(silver) AS "Total_Silver",
   SUM(bronze) AS "Total_Bronze"
FROM medals   
GROUP BY teamcountry
ORDER BY "Total_Gold" DESC;

--Number of athletes from each country
SELECT country, COUNT(*) AS "Total_Athletes" 
FROM athletes
GROUP BY country
ORDER BY "Total_Athletes" DESC;

--Top 5 countries with highest number of coaches
SELECT COUNT(name) AS "Total_Coaches", country FROM coaches
GROUP BY country
ORDER BY "Total_Coaches" DESC 
LIMIT 5

---Top 10  country with gold medals
SELECT TOP(10)teamcountry,
SUM(gold) AS Total_Gold,
SUM(silver)AS Total_Silver,
SUM(bronze) AS Total_Bronze
FROM medals
GROUP BY teamcountry
ORDER BY Total_Gold DESC;

--Avg no of medals for each discipline for each gender
SELECT discipline, 
   ROUND(AVG(female),2) AS "Average_Females",
   ROUND(AVG(male),2) AS "Average_Males"
FROM entriesgender
GROUP BY discipline;

--Number of Medals won by Turkey
SELECT teamcountry, bronze, silver, gold, total FROM medals
WHERE teamcountry = 'Turkey';

--Number of Medals won by Bulgaria
SELECT teamcountry, bronze, silver, gold, total FROM medals
WHERE teamcountry LIKE 'Bulgaria';