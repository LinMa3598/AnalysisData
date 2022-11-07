# -*-coding:utf-8-*-
import csv
import datetime
import operator
import time

from clickhouse_driver import Client, connect


class CKServer:
    def __init__(self, host, port, user, password, database, settings={}):
        self.client = Client(host=host, port=port, user=user, password=password, database=database, settings=settings)
        self.connect = connect(host=host, port=port, user=user, password=password, database=database)
        self.cursor = self.connect.cursor()

    def execute(self, sql: object, params: list) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params)
        return result

    def execute_use_setting(self, sql: object, params: list, settings) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params, settings=settings)
        return result

    def execute_no_params(self, sql: object):
        result = self.client.execute(sql)
        return result

    def fetchall(self, sql):
        result = self.client.execute(sql)
        return result

    def close(self):
        self.client.disconnect()


def get_merge_count_from_recent_years(year, if_merged, ck_info):
    host = ck_info['host']
    port = ck_info['port']
    user = ck_info['user']
    password = ck_info['password']
    database = ck_info['database']

    ck = CKServer(host=host, port=port, user=user, password=password, database=database)
    arg = ''
    if year != 'all':
        arg = f' and toYear(authored_date)>={year}'
    sql1 = f"""
    select *,splitByChar('@',author_email)[2] as email_domain from (select a.*,final_company_inferred_from_company from (select a.*,b.author__id from (select a.*,b.merge_counts from (
        WITH CAST(sumMap([area], [merge_counts]), 'Map(String, UInt32)') AS map1,CAST(sumMap([area], [alter_code_lines]), 'Map(String, UInt32)') AS map2,CAST(sumMap([area], [alter_file_counts]), 'Map(String, UInt32)') AS map3
    select search_key__owner, search_key__repo, author_email, map1,map2,map3 from (select search_key__owner, search_key__repo, author_email, area,sum(alter_code_lines) as alter_code_lines,sum(alter_file_counts) as alter_file_counts, sum(merge_counts) as merge_counts
    from (select search_key__owner, search_key__repo, author_email, '北美' as area,sum(alter_code_lines) as alter_code_lines,sum(alter_file_counts) as alter_file_counts, count() as merge_counts from
          (select search_key__owner, search_key__repo, author_email, author_tz, arraySum(`files.lines`) as alter_code_lines,length(`files.lines`) as alter_file_counts from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and author_tz global in (-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12)
              {arg})
                                                                                                                                                                    
            
          group by search_key__owner, search_key__repo, author_email, area
          order by search_key__owner, search_key__repo, author_email, merge_counts desc


          union all

          select search_key__owner, search_key__repo, author_email, '欧洲西部' as area,sum(alter_code_lines) as alter_code_lines,sum(alter_file_counts) as alter_file_counts,  count() as merge_counts
          from
        (select search_key__owner, search_key__repo, author_email, author_tz, arraySum(`files.lines`) as alter_code_lines,length(`files.lines`) as alter_file_counts from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and author_tz global in (1, 2)
            {arg})
          
          group by search_key__owner, search_key__repo, author_email, area
          order by search_key__owner, search_key__repo, author_email, merge_counts desc


          union all

          select search_key__owner, search_key__repo, author_email,'0时区' as area,sum(alter_code_lines) as alter_code_lines,sum(alter_file_counts) as alter_file_counts,  count() as merge_counts
          from
        (select search_key__owner, search_key__repo, author_email, author_tz, arraySum(`files.lines`) as alter_code_lines,length(`files.lines`) as alter_file_counts from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and author_tz global in (0)
            {arg})
          
          group by search_key__owner, search_key__repo, author_email,area
          order by search_key__owner, search_key__repo, author_email, merge_counts desc


          union all

          select search_key__owner, search_key__repo, author_email, '欧洲东部' as area,sum(alter_code_lines) as alter_code_lines,sum(alter_file_counts) as alter_file_counts,  count() as merge_counts
          from
        (select search_key__owner, search_key__repo, author_email, author_tz, arraySum(`files.lines`) as alter_code_lines,length(`files.lines`) as alter_file_counts from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and author_tz global in (3, 4)
            {arg})
          
          group by search_key__owner, search_key__repo, author_email,area
          order by search_key__owner, search_key__repo, author_email, merge_counts desc

          union all

          select search_key__owner, search_key__repo, author_email, '印度' as area,sum(alter_code_lines) as alter_code_lines,sum(alter_file_counts) as alter_file_counts,  count() as merge_counts
          from
        (select search_key__owner, search_key__repo, author_email, author_tz, arraySum(`files.lines`) as alter_code_lines,length(`files.lines`) as alter_file_counts from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and author_tz global in (5)
            {arg})
          
          group by search_key__owner, search_key__repo, author_email, area
          order by search_key__owner, search_key__repo, author_email, merge_counts desc

          union all

          select search_key__owner, search_key__repo, author_email,  '中国' as area,sum(alter_code_lines) as alter_code_lines,sum(alter_file_counts) as alter_file_counts,  count() as merge_counts
          from
        (select search_key__owner, search_key__repo, author_email, author_tz, arraySum(`files.lines`) as alter_code_lines,length(`files.lines`) as alter_file_counts from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and author_tz global in (8)
            {arg})
          
          group by search_key__owner, search_key__repo, author_email, area
          order by search_key__owner, search_key__repo, author_email, merge_counts desc


          union all

          select search_key__owner, search_key__repo, author_email, '日韩' as area,sum(alter_code_lines) as alter_code_lines,sum(alter_file_counts) as alter_file_counts,  count() as merge_counts
          from
        (select search_key__owner, search_key__repo, author_email, author_tz, arraySum(`files.lines`) as alter_code_lines,length(`files.lines`) as alter_file_counts from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and author_tz global in (9)
            {arg})
          
          group by search_key__owner, search_key__repo, author_email, area
          order by search_key__owner, search_key__repo, author_email, merge_counts desc
          limit 3 by search_key__owner,search_key__repo,author_email


          union all

          select search_key__owner, search_key__repo, author_email,  '澳洲' as area,sum(alter_code_lines) as alter_code_lines,sum(alter_file_counts) as alter_file_counts,  count() as merge_counts
          from
        (select search_key__owner, search_key__repo, author_email, author_tz, arraySum(`files.lines`) as alter_code_lines,length(`files.lines`) as alter_file_counts from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and author_tz global in (10)
            {arg})
          
          group by search_key__owner, search_key__repo, author_email,area
          order by search_key__owner, search_key__repo, author_email, merge_counts desc
)
    group by search_key__owner, search_key__repo, author_email, area
    order by search_key__owner,search_key__repo,author_email, merge_counts desc
    limit 3 by search_key__owner,search_key__repo,author_email)

    group by search_key__owner, search_key__repo, author_email
    ) as a global left join (select search_key__owner, search_key__repo, author_email, merge_counts
    from (select search_key__owner, search_key__repo, author_email, count() as merge_counts
          from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
          
          group by search_key__owner, search_key__repo, author_email
          order by search_key__owner, search_key__repo, author_email, merge_counts desc
          )
    order by search_key__owner, search_key__repo, merge_counts desc) as b on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and a.author_email = b.author_email) as a global left join (select search_key__owner, search_key__repo, commit__author__email, author__id
                              from github_commits
                              where (search_key__repo = 'rust' or
                                     search_key__repo = 'linux' or
                                     search_key__repo = 'llvm-project' or
                                     search_key__repo = 'FFmpeg' or
                                     search_key__repo = 'servo' or
                                     search_key__repo = 'o3de' or
                                     search_key__repo = 'cgal' or
                                     search_key__repo = 'qemu' or
                                     search_key__repo = 'zookeeper' or
                                     search_key__repo = 'kafka' or
                                     search_key__repo = 'redis' or
                                     search_key__repo = 'hadoop' or
                                     search_key__repo = 'spark' or
                                     search_key__repo = 'kafka' or
                                     search_key__repo = 'redis' or
                                     search_key__repo = 'hadoop' or
                                     search_key__repo = 'meetings' or
                                     search_key__repo = 'design' or
                                     search_key__repo = 'spec' or
                                     search_key__repo = 'proposals' or
                                     search_key__repo = 'gc' or
                                     search_key__repo = 'WASI')
                                and author__id != 0
                              group by search_key__owner, search_key__repo, commit__author__email, author__id) as b
    on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and a.author_email = b.commit__author__email) as a global left join
        (select id, final_company_inferred_from_company
                        from github_profile
                        where final_company_inferred_from_company != ''
                        group by id, final_company_inferred_from_company) as b on a.author__id =b.id)"""


    sql2 = f"""
    
        select *,splitByChar('@',committer_email)[2] as email_domain from (select a.*,final_company_inferred_from_company from (select a.*,b.committer__id from (select a.*,b.merge_counts from (
        WITH CAST(sumMap([area], [merge_counts]), 'Map(String, UInt32)') AS map
    select search_key__owner, search_key__repo, committer_email, map from (select search_key__owner, search_key__repo, committer_email, area, sum(merge_counts) as merge_counts
    from (select search_key__owner, search_key__repo, committer_email, committer_tz, '北美' as area, count() as merge_counts
          from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and committer_tz global in (-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12)
              {arg}
          group by search_key__owner, search_key__repo, committer_email, committer_tz
          order by search_key__owner, search_key__repo, committer_email, merge_counts desc
          limit 3 by search_key__owner,search_key__repo,committer_email


          union all

          select search_key__owner, search_key__repo, committer_email, committer_tz, '欧洲西部' as area, count() as merge_counts
          from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and committer_tz global in (1, 2)
            {arg}
          group by search_key__owner, search_key__repo, committer_email, committer_tz
          order by search_key__owner, search_key__repo, committer_email, merge_counts desc
          limit 3 by search_key__owner,search_key__repo,committer_email


          union all

          select search_key__owner, search_key__repo, committer_email, committer_tz, '0时区' as area, count() as merge_counts
          from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and committer_tz global in (0)
            {arg}
          group by search_key__owner, search_key__repo, committer_email, committer_tz
          order by search_key__owner, search_key__repo, committer_email, merge_counts desc
          limit 3 by search_key__owner,search_key__repo,committer_email


          union all

          select search_key__owner, search_key__repo, committer_email, committer_tz, '欧洲东部' as area, count() as merge_counts
          from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and committer_tz global in (3, 4)
            {arg}
          group by search_key__owner, search_key__repo, committer_email, committer_tz
          order by search_key__owner, search_key__repo, committer_email, merge_counts desc
          limit 3 by search_key__owner,search_key__repo,committer_email

          union all

          select search_key__owner, search_key__repo, committer_email, committer_tz, '印度' as area, count() as merge_counts
          from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and committer_tz global in (5)
            {arg}
          group by search_key__owner, search_key__repo, committer_email, committer_tz
          order by search_key__owner, search_key__repo, committer_email, merge_counts desc
          limit 3 by search_key__owner,search_key__repo,committer_email

          union all

          select search_key__owner, search_key__repo, committer_email, committer_tz, '中国' as area, count() as merge_counts
          from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and committer_tz global in (8)
            {arg}
          group by search_key__owner, search_key__repo, committer_email, committer_tz
          order by search_key__owner, search_key__repo, committer_email, merge_counts desc
          limit 3 by search_key__owner,search_key__repo,committer_email


          union all

          select search_key__owner, search_key__repo, committer_email, committer_tz, '日韩' as area, count() as merge_counts
          from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and committer_tz global in (9)
            {arg}
          group by search_key__owner, search_key__repo, committer_email, committer_tz
          order by search_key__owner, search_key__repo, committer_email, merge_counts desc
          limit 3 by search_key__owner,search_key__repo,committer_email


          union all

          select search_key__owner, search_key__repo, committer_email, committer_tz, '澳洲' as area, count() as merge_counts
          from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
            and committer_tz global in (10)
            {arg}
          group by search_key__owner, search_key__repo, committer_email, committer_tz
          order by search_key__owner, search_key__repo, committer_email, merge_counts desc
          limit 3 by search_key__owner,search_key__repo,committer_email)
    group by search_key__owner, search_key__repo, committer_email, area
    order by search_key__owner,search_key__repo,committer_email, merge_counts desc
    limit 3 by search_key__owner,search_key__repo,committer_email)

    group by search_key__owner, search_key__repo, committer_email
    ) as a global left join (select search_key__owner, search_key__repo, committer_email, merge_counts
    from (select search_key__owner, search_key__repo, committer_email, count() as merge_counts
          from gits
          where (search_key__repo = 'rust' or
                 search_key__repo = 'linux' or
                 search_key__repo = 'llvm-project' or
                 search_key__repo = 'FFmpeg' or
                 search_key__repo = 'servo' or
                 search_key__repo = 'o3de' or
                 search_key__repo = 'cgal' or
                 search_key__repo = 'qemu' or
                 search_key__repo = 'zookeeper' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'spark' or
                 search_key__repo = 'kafka' or
                 search_key__repo = 'redis' or
                 search_key__repo = 'hadoop' or
                 search_key__repo = 'meetings' or
                 search_key__repo = 'design' or
                 search_key__repo = 'spec' or
                 search_key__repo = 'proposals' or
                 search_key__repo = 'gc' or
                 search_key__repo = 'WASI')
            and if_merged = {if_merged}
          
          group by search_key__owner, search_key__repo, committer_email
          order by search_key__owner, search_key__repo, committer_email, merge_counts desc
          )
    order by search_key__owner, search_key__repo, merge_counts desc) as b on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and a.committer_email = b.committer_email) as a global left join (select search_key__owner, search_key__repo, commit__committer__email, committer__id,committer__id
                              from github_commits
                              where (search_key__repo = 'rust' or
                                     search_key__repo = 'linux' or
                                     search_key__repo = 'llvm-project' or
                                     search_key__repo = 'FFmpeg' or
                                     search_key__repo = 'servo' or
                                     search_key__repo = 'o3de' or
                                     search_key__repo = 'cgal' or
                                     search_key__repo = 'qemu' or
                                     search_key__repo = 'zookeeper' or
                                     search_key__repo = 'kafka' or
                                     search_key__repo = 'redis' or
                                     search_key__repo = 'hadoop' or
                                     search_key__repo = 'spark' or
                                     search_key__repo = 'kafka' or
                                     search_key__repo = 'redis' or
                                     search_key__repo = 'hadoop' or
                                     search_key__repo = 'meetings' or
                                     search_key__repo = 'design' or
                                     search_key__repo = 'spec' or
                                     search_key__repo = 'proposals' or
                                     search_key__repo = 'gc' or
                                     search_key__repo = 'WASI')
                                and committer__id != 0
                              group by search_key__owner, search_key__repo, commit__committer__email, committer__id) as b
    on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and a.committer_email = b.commit__committer__email) as a global left join
        (select id, final_company_inferred_from_company
                        from github_profile
                        where final_company_inferred_from_company != ''
                        group by id, final_company_inferred_from_company) as b on a.committer__id =b.id)
"""
    if if_merged == 0:
        results = ck.execute_no_params(sql=sql1)
    if if_merged == 1:
        results = ck.execute_no_params(sql=sql2)
    # print(results)
    print(sql1)
    filename = ''
    if if_merged == 1 and year != 'all':
        filename = f"merge_since_{year}_{datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%SZ')}.csv"
    elif if_merged == 1 and year == 'all':
        filename = f"merge_{datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%SZ')}.csv"
    elif if_merged == 0 and year != 'all':
        filename = f"contribute_since_{year}_{datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%SZ')}.csv"
    elif year == 'all':
        filename = f"all_years_contribute_{datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%SZ')}.csv"
    with open(filename, "w", encoding='utf8',
              newline='') as csvfile:
        writer = csv.writer(csvfile)

        # 先写入columns_name
        if if_merged == 0:
            writer.writerow(["org_repo",
                             "email",
                             "area_1",
                             "area_1_commit_count",
                             "area_1_alter_code_lines",
                             "area_1_alter_file_counts",
                             "area_2",
                             "area_2_commit_count",
                             "area_2_alter_code_lines",
                             "area_2_alter_file_counts",
                             "area_3",
                             "area_3_commit_count",
                             "area_3_alter_code_lines",
                             "area_3_alter_file_counts",
                             "total_commit_count",
                             "github_id",
                             "company_inferrd_from_profile",
                             "email_domain"
                             ])

        for result in results:
            row = []
            owner = result[0]
            repo = result[1]
            email = result[2]
            area_commit_map = result[3]
            area_alter_code_map = result[4]
            area_alter_file_map = result[5]
            for i in area_commit_map:
                try:
                    area_commit_map[i] = [area_commit_map[i], area_alter_code_map.get(i,0), area_alter_file_map[i]]
                except Exception as e:
                    print(area_commit_map, area_alter_code_map, area_alter_file_map)
            # print(area_commit_map)
            # return
            total_merge_count = result[6]
            github_id = result[7]
            company = result[8]
            emain_domain = result[9]
            # print(company)
            area_commit_map = sorted(area_commit_map.items(), key=lambda x: x[1][0], reverse=True)
            # tz_list = int(author_tz.keys)
            # tz_merge_count = author_tz.values
            # print(tz_list)

            author_area_1 = ''
            author_area_2 = ''
            author_area_3 = ''
            area_1_commit_count = 0
            area_2_commit_count = 0
            area_3_commit_count = 0
            area_1_alter_code_count = 0
            area_2_alter_code_count = 0
            area_3_alter_code_count = 0
            area_1_alter_file_count = 0
            area_2_alter_file_count = 0
            area_3_alter_file_count = 0
            # print(author_tz)
            if len(area_commit_map) == 1:
                author_area_1 = area_commit_map[0][0]
                area_1_commit_count = area_commit_map[0][1][0]
                area_1_alter_code_count = area_commit_map[0][1][1]
                area_1_alter_file_count = area_commit_map[0][1][2]
            elif len(area_commit_map) == 2:
                author_area_1 = area_commit_map[0][0]
                area_1_commit_count = area_commit_map[0][1][0]
                area_1_alter_code_count = area_commit_map[0][1][1]
                area_1_alter_file_count = area_commit_map[0][1][2]
                author_area_2 = area_commit_map[1][0]
                area_2_commit_count = area_commit_map[1][1][0]
                area_2_alter_code_count = area_commit_map[1][1][1]
                area_2_alter_file_count = area_commit_map[1][1][2]
            elif len(area_commit_map) == 3:
                author_area_1 = area_commit_map[0][0]
                area_1_commit_count = area_commit_map[0][1][0]
                area_1_alter_code_count = area_commit_map[0][1][1]
                area_1_alter_file_count = area_commit_map[0][1][2]
                author_area_2 = area_commit_map[1][0]
                area_2_commit_count = area_commit_map[1][1][0]
                area_2_alter_code_count = area_commit_map[1][1][1]
                area_2_alter_file_count = area_commit_map[1][1][2]
                author_area_3 = area_commit_map[2][0]
                area_3_commit_count = area_commit_map[2][1][0]
                area_3_alter_code_count = area_commit_map[2][1][1]
                area_3_alter_file_count = area_commit_map[2][1][2]
            row.append(owner+'_'+repo)
            row.append(email)
            row.append(author_area_1)
            row.append(area_1_commit_count)
            row.append(area_1_alter_code_count)
            row.append(area_1_alter_file_count)
            row.append(author_area_2)
            row.append(area_2_commit_count)
            row.append(area_2_alter_code_count)
            row.append(area_2_alter_file_count)
            row.append(author_area_3)
            row.append(area_3_commit_count)
            row.append(area_3_alter_code_count)
            row.append(area_3_alter_file_count)
            row.append(total_merge_count)
            row.append(github_id)
            row.append(company)
            row.append(emain_domain)
            writer.writerow(row)
        # break


if __name__ == '__main__':

    # host = input("请输入数据库ip:")
    # port = input("请输入数据库port:")
    # user = input("请输入用户名:")
    # password = input("请输入密码:")
    # database = input("请输入数据库名字:")
    host = ''
    port = 0
    user = ''
    password = ''
    database = ''
    ck_info = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database
    }

    for year in [2020, 2021, 2022, 'all']:
        # if_merged = 1
        # get_merge_count_from_recent_years(year, if_merged, ck_info)
        # time.sleep(2)
        if_merged = 0
        get_merge_count_from_recent_years(year, if_merged, ck_info)
        time.sleep(2)
    # get_merge_count_from_recent_years(2018, 1,ck_info=ck_info)
