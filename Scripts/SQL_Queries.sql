-- How long does it take for questions to receive answers ?
select id, ques_creation_date, answer_creation_date, time_diff
from
	(Select ai_posts.id, ai_posts.creationdate as ques_creation_date
	 		,ai_posts.posttypeid,
			x.creationdate as answer_creation_date,
	 		x.posttypeid,
			x.creationdate::timestamp - ai_posts.creationdate::timestamp as time_diff,
			rank() over(PARTITION BY ai_posts.id ORDER BY x.creationdate::timestamp - ai_posts.creationdate::timestamp) as rank
	from ai_posts
	join (select * from ai_posts where posttypeid = 2) x
	 on ai_posts.id = x.parentid) subt
where subt."rank" = 1

-- what is the percentage of questions that have been answered? 
select (Select count(*) from ai_posts
where posttypeid=1 and answercount>0)/(Select count(*) from ai_posts)::float*100 as percentage

-- Wich day of the week has the most answers of questions with an hour?
select count(id) as c1,
to_char(creationdate, 'Day') as day,
date_part('hour', creationdate) as hour
from ai_posts
where posttypeid=2
group by day, hour
order by c1 desc
limit 1