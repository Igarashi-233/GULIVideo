--统计硅谷影音视频网站的常规指标，各种TopN指标：

gulivideo_orc:
	videoId string, 
    uploader string, 
    age int, 
    category array<string>, 
    length int, 
    views int, 
    rate float, 
    ratings int, 
    comments int,
    relatedId array<string>

--统计视频观看数Top10
select videoId,
views
from gulivideo_orc
order by views desc
limit 10;


--统计视频类别热度Top10(某类视频个数作为其热度)
1.使用UDTF将类别炸开
select videoId,
category_name
from gulivideo_orc
lateral view explode(category) tmp_category as category_name;  ----T1

2.按照category_name进行分组，统计每种类别视频的总数，并且按照总数进行倒序排名
select category_name,
count(*) category_count
from (select videoId,
category_name
from gulivideo_orc
lateral view explode(category) tmp_category as category_name)T1
group by category_name
order by category_count desc
limit 10;


--统计视频观看数Top20所属类别以及类别包含的Top20的视频个数
1.统计前20观看数的视频
select videoId,
views,
category
from gulivideo_orc
order by views desc
limit 20;  -----T1

2.对T1表中的category进行炸裂
select videoId,
category_name
from
(select videoId,
views,
category
from gulivideo_orc
order by views desc
limit 20)T1
lateral view explode(category) tmp_category as category_name;  -----T2

3.求类别包含Top20视频个数
select category_name,
count(*) category_count
from 
(select videoId,
category_name
from
(select videoId,
views,
category
from gulivideo_orc
order by views desc
limit 20)T1
lateral view explode(category) tmp_category as category_name)T2
group by category_name
order by category_count desc;


--统计视频观看数Top50所关联视频的所属类别Rank
1.视频观看数前50
select views,
relatedId
from gulivideo_orc
order by views desc
limit 50;  -----T1

2.对T1表中的relatedId进行炸裂
select related_id
from
(select views,
relatedId
from gulivideo_orc
order by views desc
limit 50)T1
lateral view explode(relatedId) tmp_relatedId as related_id
group by related_id;  -----T2

3.取出关联视频类别
select T2.related_id,
orc.category
from 
(select related_id
from
(select views,
relatedId
from gulivideo_orc
order by views desc
limit 50)T1
lateral view explode(relatedId) tmp_relatedId as related_id
group by related_id)T2
join
gulivideo_orc orc
on
T2.related_id=orc.videoId;   -----T3

4.对T3表中的category进行炸裂
select explode(category) category_name
from
(select T2.related_id,
orc.category
from 
(select related_id
from
(select views,
relatedId
from gulivideo_orc
order by views desc
limit 50)T1
lateral view explode(relatedId) tmp_relatedId as related_id
group by related_id)T2
join
gulivideo_orc orc
on
T2.related_id=orc.videoId)T3;   -----T4

5.分组(类别)求和,排序
select category_name,
count(*) category_count
from
(select explode(category) category_name
from
(select T2.related_id,
orc.category
from 
(select related_id
from
(select views,
relatedId
from gulivideo_orc
order by views desc
limit 50)T1
lateral view explode(relatedId) tmp_relatedId as related_id
group by related_id)T2
join
gulivideo_orc orc
on
T2.related_id=orc.videoId)T3)T4
group by category_name
order by category_count desc;



gulivideo_category:
--统计每个类别中视频流量Top10
1.给每一种类别根据视频流量添加rank值
select categoryId,
ratings,
videoId,
dense_rank() over(partition by categoryId order by ratings desc) rk
from gulivideo_category;   -----T1

2.过滤前十
select categoryId,
videoId,
ratings
from
(select categoryId,
ratings,
videoId,
dense_rank() over(partition by categoryId order by ratings desc) rk
from gulivideo_category)T1
where
rk<=10;


--统计每个类别视频观看数Top10
1.给每一种类别根据视频观看数添加rank值
select categoryId,
views,
videoId,
dense_rank() over(partition by categoryId order by views desc) rk
from gulivideo_category;   -----T1

2.过滤前十
select categoryId,
views,
videoId
from
(select categoryId,
views,
videoId,
dense_rank() over(partition by categoryId order by views desc) rk
from gulivideo_category)T1
where
rk<=10;



gulivideo_user_orc:
	uploader string,
    videos int,
    friends int
	
--统计上传视频最多的用户Top10以及他们上传的观看次数在前20的视频
1.统计上传视频最多的用户Top10
select uploader,
videos
from gulivideo_user_orc
order by videos desc
limit 10;   -----T1

2.取出这10人上传的所有视频,按照观看次数排名，取前20
select v.videoId,
v.uploader,
v.views
from gulivideo_orc v
join
(select uploader,
videos
from gulivideo_user_orc
order by videos desc
limit 10)T1
on
v.uploader=T1.uploader
order by v.views desc
limit 20;






























