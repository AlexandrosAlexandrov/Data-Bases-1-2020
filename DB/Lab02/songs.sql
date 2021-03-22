select etaireia,count(*)  from cd_production group by etaireia;
select titlos from tragoudi where sinthetis=stixourgos;
select title from singer_prod group by title having count(*)>1;
select tragoudistis,count(distinct cd) from singer_prod group by tragoudistis;
select s.title from singer_prod s, group_prod g where s.title=g.title;
select k.onoma, k.epitheto from kalitexnis k, singer_prod s, tragoudi t where s.title=t.titlos and t.sinthetis=k.ar_taut group by k.onoma, k.epitheto having count(distinct s.tragoudistis)=1;
select title from group_prod where title not in(select title from singer_prod)