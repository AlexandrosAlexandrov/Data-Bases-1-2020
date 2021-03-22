#1
select name, review_count, yelping_since from user where name="Lisa"and review_count>500 group by yelping_since;

#2
select review_id from reviews where user_id in
					(select user_id from user where name="Lisa")
                    and business_id in
					(select business_id
					from business where name="Gab & Eat");

#3
create table yn(
	answer varchar(30) 
);
insert into yn (answer) value ("yes");
insert into yn (answer) value ("no");
select * from yn;
select y.answer from reviews_pos_neg p, yn y where p.review_id in (
					select review_id from reviews where business_id="OmpbTu4deR3ByOo7btTTZw")
                    and p.positive=1;

#4
select r.business_id, count(p.positive) from reviews r, reviews_pos_neg p where r.review_id in(
					select review_id from reviews_pos_neg)
                    and r.date between '2014-01-01' and '2014-12-31'
                    having count(p.positive)>10;
                    
#5                    
select user_id, count(*) from reviews where business_id in(
					select business_id from business where business_id in(
						select business_id from business_category where category_id in(
							select category_id from category where category="Mobile Phones")))
					group by user_id;

#6                    
select user_id, votes_useful from reviews where business_id in(
					select business_id from business where name="Midas")
                    order by votes_useful desc;
									