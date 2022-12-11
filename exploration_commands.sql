-- importing the data from the CSV sheet. Using Load Data is relatively much faster than using the import wizard

LOAD DATA  LOCAL INFILE '<location>/covid-hospitalizations.csv' INTO TABLE covidhosp
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


LOAD DATA  LOCAL INFILE '<location>/covid-vaccination.csv' INTO TABLE covidvacc
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


-- Make sure to check if the count from both the tables are the same
select count(*) from covidhosp;
select count(*) from covidvacc;


-- Step by explorations; constraint the statement by limit or where conditions to limit the number of results.

select location, date, total_cases, new_cases, total_deaths, population 
from covid_explorations.covidhosp;



-- The default date used in the dataset does not follow the MySQL format. You can format it and cast it. Otherwise, have to do string operations.

select str_to_date(date, '%m/%d/%Y') 
from covid_explorations.covidvacc;

-- I choose to update the table to follow that format. You can also alter the schema if you would like additionally. 

-- If you come across the Error Code: 1175. You are using safe update mode and you tried to update a table without a WHERE that uses a KEY column.
-- Make sure to set the safe update to 1 and then reset it back after you are done to be safe.

SET SQL_SAFE_UPDATES = 0;

update covid_explorations.covidvacc 
set date = str_to_date(date, '%m/%d/%Y');

select str_to_date(date, '%m/%d/%Y') 
from covid_explorations.covidvacc;

update covid_explorations.covidvacc 
set date = str_to_date(date, '%m/%d/%Y');

SET SQL_SAFE_UPDATES = 1;

-- Get the total deaths per total number of cases (death and infected) percentages. I have limited the results to a specific country. 
select location, date, total_cases, total_deaths, round((total_deaths/ total_cases) * 100, 2) as death_percentage
from covid_explorations.covidhosp
where location like "%india%"
order by 1,2; 


select location, date, total_cases, population, round((total_cases/ population) * 100, 2) as infected_percentage
from covid_explorations.covidhosp
where location like "%India%"
order by 1,2; 

-- Now to understand the comparison of population to infection and death rates, the following commands could be used. 
select location, population, max(total_cases) as highest_infection, max(round((total_cases/ population) * 100, 2)) as highest_infected_percentage
from covid_explorations.covidhosp
where continent <> ''
group by population, location
order by highest_infected_percentage desc; 


select location, population, max(cast(total_deaths as unsigned)) as highest_death, max(round((total_deaths/ population) * 100, 2)) as highest_death_percentage
from covid_explorations.covidhosp
where continent <> ''
group by location, population
order by highest_death desc; 

-- Also to understand the death percentage with respect to continents

select location, max(cast(total_deaths as unsigned)) as highest_death, max(round((total_deaths/ population) * 100, 2)) as highest_death_percentage
from covid_explorations.covidhosp
where continent <> ''
group by location
order by highest_death desc; 


-- Something I noticed along the lines while exploring the db is that there are location elements such as 'World', 'low middle income' which does not make much sense as location.
-- They seem to also occur when the continent value is null. This is good to know so that they can be excluded while performing any aggregations

select distinct location from covid_explorations.covidhosp where continent ='';

-- Considering only the continents based on the above statement

select location, max(cast(total_deaths as unsigned)) as total_deaths_count 
from covid_explorations.covidhosp
where continent = '' and location not like '%income%' and location not like '%International%'
group by location
order by total_deaths_count desc;


-- We can also try to get the overall numbers grouped by date or total

select date, sum(total_cases) as case_count, sum(total_deaths) as deaths_count, round(sum(total_deaths)/sum(total_cases),2) as death_percentage
from covid_explorations.covidhosp
where continent <> '' and location not like '%income%' and location not like '%International%'
group by date;

select  sum(total_cases) as case_count, sum(total_deaths) as deaths_count, round(sum(total_deaths)/sum(total_cases),2) as death_percentage
from covid_explorations.covidhosp
where continent <> '' and location not like '%income%' and location not like '%International%';


-- To also understand the impact of vaccination in relation with the cases.

select vacc.date, hosp.location, hosp.continent, hosp.population, vacc.new_vaccinations
from covid_explorations.covidhosp hosp
join covid_explorations.covidvacc vacc
on hosp.location = vacc.location and hosp.date = vacc.date
where vacc.location like '%India%' and hosp.continent <> '' and hosp.location not like '%income%' and hosp.location not like '%International%';


-- Over here, we are partitioning it based on cummulative vaccination upto the date ordered by the date. 

select vacc.date, hosp.location, hosp.continent, hosp.population, vacc.new_vaccinations, sum(cast(vacc.new_vaccinations as unsigned)) 
over (partition by hosp.location order by hosp.date) as cummulative_vaccinations
from covid_explorations.covidhosp hosp
join covid_explorations.covidvacc vacc
on hosp.location = vacc.location and hosp.date = vacc.date
where hosp.location like '%India%' and hosp.continent <> '' and hosp.location not like '%income%' and hosp.location not like '%International%';


 


-- Using CTE (Common Table Expression), we are adding another column with which we can also calculate the vaccination percentage. 
with cte_pop_vacc (date, continent, location, population, new_vaccinations, cummulative_vaccinations)
as
(
select vacc.date, hosp.location, hosp.continent, hosp.population, vacc.new_vaccinations, sum(cast(vacc.new_vaccinations as unsigned)) 
over (partition by hosp.location order by hosp.date) as cummulative_vaccinations
from covid_explorations.covidhosp hosp
join covid_explorations.covidvacc vacc
on hosp.location = vacc.location and hosp.date = vacc.date
where hosp.location like '%India%' and hosp.continent <> '' and hosp.location not like '%income%' and hosp.location not like '%International%'

)
select * , (cummulative_vaccinations/population)*100 as vacc_percentage
from cte_pop_vacc;





-- Creating views might help us towards generating virtual tables that is helpful for overall analysis/ visualization. 
create view percent_vaccinated as
with cte_pop_vacc (date, continent, location, population, new_vaccinations, cummulative_vaccinations)
as
(
select vacc.date, hosp.location, hosp.continent, hosp.population, vacc.new_vaccinations, sum(cast(vacc.new_vaccinations as unsigned)) 
over (partition by hosp.location order by hosp.date) as cummulative_vaccinations
from covid_explorations.covidhosp hosp
join covid_explorations.covidvacc vacc
on hosp.location = vacc.location and hosp.date = vacc.date
where hosp.location like '%India%' and hosp.continent <> '' and hosp.location not like '%income%' and hosp.location not like '%International%'

)
select * , (cummulative_vaccinations/population)*100 as vacc_percentage
from cte_pop_vacc;



-- More to come..
