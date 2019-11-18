-- https://lagunita.stanford.edu/courses/DB/SQL/SelfPaced/course/
-- SQL Social-Network Query Exercises

-- q1
-- Find the names of all students who are friends with someone named Gabriel.
select h.name as name1
from Highschooler h
join Friend f
on h.id=f.id1
join Highschooler h2
on h2.id=f.id2
where h2.name='Gabriel';

-- q2
-- For every student who likes someone 2 or more grades younger than themselves, return that student's name and grade, and the name and grade of the student they like.
select h1.name,h1.grade,h2.name,h2.grade
from likes l
join highschooler h1
on l.id1 = h1.id
join highschooler h2
on l.id2 = h2.id
where h1.grade-h2.grade>1;

-- q3
-- For every pair of students who both like each other, return the name and grade of both students. Include each pair only once, with the two names in alphabetical order.
select h1.name,h1.grade,h2.name,h2.grade
from (
select l1.id1,l1.id2
from likes l1
join likes l2
on l1.id2 = l2.id1
where l1.id1=l2.id2
) t
join highschooler h1
on t.id1=h1.id
join highschooler h2
on t.id2=h2.id
where h1.name<h2.name;

-- q4
-- Find all students who do not appear in the Likes table (as a student who likes or is liked) and return their names and grades. Sort by grade, then by name within each grade.
select name,grade
from highschooler
where id not in (select id1 from likes)
and id not in (select id2 from likes)
order by grade, name;

-- q5
-- For every situation where student A likes student B, but we have no information about whom B likes (that is, B does not appear as an ID1 in the Likes table), return A and B's names and grades.
select h1.name,h1.grade,h2.name,h2.grade
from
(
select id1,id2
from likes
where id2 not in (select id1 from likes)
) tmp
join highschooler h1
on tmp.id1 = h1.id
join highschooler h2
on tmp.id2 = h2.id;

-- q6
-- Find names and grades of students who only have friends in the same grade. Return the result sorted by grade, then by name within each grade.
select h.name,h.grade
from highschooler h
where h.id not in
(
select f.id1
from friend f
join highschooler h1
on f.id1 = h1.id
join highschooler h2
on f.id2 = h2.id
where h1.grade != h2.grade
)
order by grade, name

-- q7
-- For each student A who likes a student B where the two are not friends, find if they have a friend C in common (who can introduce them!). For all such trios, return the name and grade of A, B, and C.
select h1.name,h1.grade,h3.name,h3.grade,h2.name,h2.grade
from ((
select f1.id1 as id1,f1.id2 as id2,f2.id2 as id3
from friend f1
join friend f2
on f1.id2=f2.id1
and f2.id2 not in (select id2 from friend where id1 = f1.id1)
where f1.id1 != f2.id2
) t
join likes l
on t.id1 = l.id1
and t.id3 = l.id2 ) t3
join highschooler h1
on t3.id1 = h1.id
join highschooler h2
on t3.id2 = h2.id
join highschooler h3
on t3.id3 = h3.id

-- q8
-- Find the difference between the number of students in the school and the number of different first names.
select cnt1-cnt2
from (select count(*) as cnt1, count(distinct name) as cnt2 from highschooler);

-- q9
-- Find the name and grade of all students who are liked by more than one other student.
select h.name,h.grade
from (
select id2, count(*) as cnt
from likes
group by id2) t
join highschooler h
on t.id2 = h.id
where t.cnt>1;













-- SQL Social-Network Modification Exercises

-- q1
-- It's time for the seniors to graduate. Remove all 12th graders from Highschooler.
Delete from highschooler where grade =12;

-- q2
-- If two students A and B are friends, and A likes B but not vice-versa, remove the Likes tuple.
delete from likes
where id1 in
(
select l1.id1
from friend f
join likes l1
on f.id2 = l1.id2
and f.id1 = l1.id1
)
and id2 not in
(
select l2.id2
from likes l1
join likes l2
on l1.id2 = l2.id1
where l1.id1 = l2.id2
);

-- q3
-- For all cases where A is friends with B, and B is friends with C, add a new friendship for the pair A and C. Do not add duplicate friendships, friendships that already exist, or friendships with oneself. (This one is a bit challenging; congratulations if you get it right.)
insert into friend(id1,id2)
select distinct f1.id1,f2.id2
from friend f1
join friend f2
on f1.id2 = f2.id1
where f1.id1 != f2.id2
and f2.id2 not in (select id2 from friend where id1 = f1.id1)
and f1.id1 not in (select id1 from friend where id2 = f2.id2);










-- SQL Movie-Rating Query Exercises

-- q1
-- Find the titles of all movies directed by Steven Spielberg.
select title from movie where director = 'Steven Spielberg';

-- q2
-- Find all years that have a movie that received a rating of 4 or 5, and sort them in increasing order.
select distinct year
from rating
join movie
on rating.mid = movie.mid
where stars = 4 or stars = 5
order by year;

-- q3
-- Find the titles of all movies that have no ratings.
select title
from movie
where mid not in
(
select r.mid
from rating r
join movie m
on r.mid = m.mid
);

-- q4
-- Some reviewers didn't provide a date with their rating. Find the names of all reviewers who have ratings with a NULL value for the date.
select w.name
from rating r
join reviewer w
on r.rid=w.rid
where r.ratingdate is null;

-- q6
-- Write a query to return the ratings data in a more readable format: reviewer name, movie title, stars, and ratingDate. Also, sort the data, first by reviewer name, then by movie title, and lastly by number of stars.
select w.name, m.title, r.stars, r.ratingdate
from rating r
join reviewer w
on r.rid = w.rid
join movie m
on r.mid = m.mid
order by
w.name,m.title,r.stars;

-- q6
-- For all cases where the same reviewer rated the same movie twice and gave it a higher rating the second time, return the reviewer's name and the title of the movie.
select w.name, m.title
from (
select r1.rid,r1.mid
from rating r1
join rating r2
on r1.rid=r2.rid
and r1.mid=r2.mid
and r1.ratingdate<r2.ratingdate
and r1.stars<r2.stars) t
join reviewer w
on t.rid=w.rid
join movie m
on t.mid=m.mid;

-- q7
-- For each movie that has at least one rating, find the highest number of stars that movie received. Return the movie title and number of stars. Sort by movie title.
select m.title, t.max_stars
from (
select mid, count(*) as cnt, max(stars) as max_stars
from rating
group by mid ) t
join movie m
on t.mid=m.mid
where t.cnt>0
order by m.title;

-- q8
-- For each movie, return the title and the 'rating spread', that is, the difference between highest and lowest ratings given to that movie. Sort by rating spread from highest to lowest, then by movie title.
select m.title,t.spread
from
(
select mid, max(stars) as maxx, min(stars) as minn, (max(stars)-min(stars)) as spread
from rating
group by mid
) t
join movie m
on t.mid=m.mid
order by spread desc, title;

-- q9
-- Find the difference between the average rating of movies released before 1980 and the average rating of movies released after 1980. (Make sure to calculate the average rating for each movie, then the average of those averages for movies before 1980 and movies after. Don't just calculate the overall average rating before and after 1980.)
select abs(star_b41980-star_af1980)
from
(
select avg(avg_stars) as star_b41980
from (
select mid, avg(stars) as avg_stars
from rating
group by mid ) t
join movie m
on t.mid=m.mid
where m.year<1980
) tmp1,
(
select avg(avg_stars) as star_af1980
from (
select mid, avg(stars) as avg_stars
from rating
group by mid ) t
join movie m
on t.mid=m.mid
where m.year>1980
) tmp2;







-- SQL Movie-Rating Modification Exercises

-- q1
-- Add the reviewer Roger Ebert to your database, with an rID of 209.
insert into reviewer(name,rid)
values('Roger Ebert',209 );

-- q2
-- Insert 5-star ratings by James Cameron for all movies in the database. Leave the review date as NULL.
insert into rating(stars, mid, rid, ratingDate)
select 5, m.mid,(select rid from reviewer where name = 'James Cameron'), null
from movie m;

-- q3
-- For all movies that have an average rating of 4 stars or higher, add 25 to the release year. (Update the existing tuples; don't insert new tuples.)
update movie
set year = year+25
where mid in (
select mid
from
(select mid,avg(stars) as avg_stars
from rating
group by mid
) t
where t.avg_stars>=4);

-- q4
-- Remove all ratings where the movie's year is before 1970 or after 2000, and the rating is fewer than 4 stars.
delete from rating
where rating.mid in
(
select m.mid
from movie m
where m.year<1970
or m.year>2000
)
and rating.stars<4;















