-- 从git角度所有邮箱
-- 某个公司邮箱后缀在某个项目的commit数量 （取每种公司邮箱后缀commit数量排行前20的项目）
with ('google.com','huawei.com') as email_domains
 select *
 from (select email_domain, search_key__owner, search_key__repo,count() as commit_count
       from gits
       where if_merged = 0
         and email_domain != ''
       group by splitByChar('@', author_email)[2] as email_domain, search_key__owner, search_key__repo
       order by email_domain,commit_count desc ) where email_domain global in email_domains
limit 20 by email_domain
;



--从 github 角度 对出账号的邮箱
-- 某个公司的邮箱后缀在某个项目中的开发者人数（取每种公司邮箱后缀contributor 数量排行前20的项目），
with ('google.com', 'huawei.com') as email_domains
 select email_domain, search_key__owner, search_key__repo, count() as contributor_count
 from (
       select *
       from (
                select author__id, search_key__owner, search_key__repo, commit__author__email
                from github_commits
                where author__id != 0
                group by author__id, search_key__owner, search_key__repo, commit__author__email
                )
       where splitByChar('@', commit__author__email)[2] global in email_domains)
 group by splitByChar('@', commit__author__email)[2] as email_domain, search_key__owner, search_key__repo
 order by email_domain, contributor_count desc
limit 20 by email_domain




;
-- 对出账号的邮箱email_domain数量排行
 select email_domain, count() as email_count
 from (select commit__author__email from github_commits where author__id != 0 group by commit__author__email)
 group by splitByChar('@', commit__author__email)[2] as email_domain
 order by email_count desc