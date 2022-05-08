
/****** Object:  StoredProcedure [playpen].[p_url_splitting]    Script Date: 02/04/2022 19:05:14 ******/
SET ANSI_NULLS ON
GO

/*

All tables: 

select distinct hist_date from [playpen].[jm_accuranker_keywords] order by hist_date 

select top 100 * from [playpen].[jm_accuranker_domain] 

select top 100 * from [playpen].[jm_accuranker_domain_historic] 
order by id, hist_date

select top 100 * from [playpen].[jm_accuranker_keywords]
order by domain, keyword, hist_date

select top 100 * from [playpen].[jm_accuranker_keywords_most_recent]		-- take a look

select top 100 * from [playpen].[jm_accuranker_keywords_search_volume]

select top 100 * from [playpen].[jm_accuranker_keywords_competitors]
order by domain, keyword, competitor_id, hist_date

																			-- tbc - combined keywords table


select top 100 * from [playpen].[jm_accuranker_keywords_tag_combo]
order by keyword_id

select top 100 * from [playpen].[jm_accuranker_landing_page]
order by landing_page_id, hist_date											-- Create simple lookup table

select top 100 * from [playpen].[jm_accuranker_tags]
where tag is not null
order by tag, hist_date	


*/


SET QUOTED_IDENTIFIER ON
GO

alter PROC [playpen].[accuranker_external_to_internal_import] AS
-- exec [playpen].[accuranker_external_to_internal_import]

--------------------------------------------------------------------------------------------------------------------------------------
-- Start of Procedure
--------------------------------------------------------------------------------------------------------------------------------------

BEGIN
BEGIN TRY

SET NOCOUNT ON;



--------------------------------------------------------------------------------------------------------------------------------------
-- Log Start
--------------------------------------------------------------------------------------------------------------------------------------

DECLARE	@Rec_Count         BigInt;

DECLARE @Batch_No_PV       Uniqueidentifier,	--PV to indicate passing value
        @Proc_Name_PV      VarChar(100),
        @Proc_Call_PV      VarChar(1000),
        @Error_Detail_PV   VarChar(8000),
        @Step_Name_PV      VarChar(500);

SELECT @Batch_No_PV    = NEWID(),
       @Proc_Name_PV   = '[playpen].[accuranker_external_to_internal_import]',
       @Proc_Call_PV   = 'EXEC [playpen].[accuranker_external_to_internal_import]';
	   
EXECUTE MONITOR.P_JOB_LOG @Batch_No       = @Batch_No_PV,		
                          @Proc_Name      = @Proc_Name_PV,		
                          @Proc_Call      = @Proc_Call_PV,		
                          @Exe_Status     = 'STARTED',
                          @Error_Detail   = NULL;


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 1.5 - Add new domains from the external table into the base tables
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 1.5 - Add new domains from the external table into the base tables',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;


insert into playpen.jm_accuranker_domain
select
	*
from
	playpen.jm_accuranker_ext_domain
where
	id not in(select distinct id from playpen.jm_accuranker_ext_domain)

print 'step 1.5 done'


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 2.5 - Add new historic domains from the external table into the base tables
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 2.5 - Add new historic domains from the external table into the base tables',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;
							   
							   
delete from playpen.jm_accuranker_domain_historic
where hist_date in(select distinct convert(date, hist_date) from playpen.jm_accuranker_ext_domain_historic)

insert into playpen.jm_accuranker_domain_historic
select
	id
	,domain
	,hist_date
	,display_name
	,convert(float,shareofvoice) as shareofvoice
	,convert(float,shareofvoicepercentage) as shareofvoicepercentage
	,convert(float,averagerank) as averagerank
	,convert(int,winners) as winners
	,convert(float,winners_shareofvoice) as winners_shareofvoice
	,convert(int,losers) as losers
	,convert(float,losers_shareofvoice) as losers_shareofvoice
	,convert(int,nomovement) as nomovement
	,convert(float,nomovement_shareofvoice) as nomovement_shareofvoice
	,convert(int,keywords_0_3) as keywords_0_3
	,convert(int,keywords_4_10) as keywords_4_10
	,convert(int,keywords_11_20) as keywords_11_20
	,convert(int,keywords_21_50) as keywords_21_50
	,convert(int,keywords_above_50) as keywords_above_50
	,convert(int,keywords_unranked) as keywords_unranked
	,convert(int,keywords_total) as keywords_total
from
	playpen.jm_accuranker_ext_domain_historic

print 'step 2.5 done'


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 3.5 - Add new tags from the external table into the base tables
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 3.5 - Add new tags from the external table into the base tables',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;


delete from playpen.jm_accuranker_tags
where hist_date in(select distinct convert(date, hist_date) from playpen.jm_accuranker_ext_tags)

insert into playpen.jm_accuranker_tags
select
	replace(replace(domain, '[',''),']','') as domain
	,tag
	,convert(date, hist_date) as hist_date
	,convert(float, averagerank) as averagerank
	,convert(float, shareofvoice) as shareofvoice
	,convert(float, shareofvoicepercentage) as shareofvoicepercentage
	,convert(float, search_volume) as search_volume
	,convert(float, keywords) as keywords
from
	playpen.jm_accuranker_ext_tags

print 'step 3.5 done'


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 4.5 - Add new landing_page from the external table into the base tables
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 4.5 - Add new landing_page from the external table into the base tables',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;


delete from playpen.jm_accuranker_landing_page
where hist_date in(select distinct convert(date, hist_date) from playpen.jm_accuranker_ext_allLandingPages)

insert into playpen.jm_accuranker_landing_page
select
	replace(replace(domain, '[',''),']','') as domain
	,landing_page_id
	,landing_page_path
	,convert(date, created_at) as created_at
	,convert(date, hist_date) as hist_date
	,convert(float, averagerank) as averagerank
	,convert(float, shareofvoice) as shareofvoice
	,convert(float, shareofvoicepercentage) as shareofvoicepercentage
	,convert(float, search_volume) as search_volume
	,convert(float, keywords) as keywords
from
	playpen.jm_accuranker_ext_allLandingPages

print 'step 4.5 done'


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 5.5 - Add new keyword from the external table into the base tables
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 5.5 - Add new keyword from the external table into the base tables',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;
							   
--select distinct title_desc from playpen.jm_accuranker_keywords
--select * from playpen.jm_accuranker_ext_allkeywords

delete from playpen.jm_accuranker_keywords
where hist_date in(select distinct convert(date, hist_date) from playpen.jm_accuranker_ext_allkeywords)

insert into playpen.jm_accuranker_keywords
select
	replace(replace(domain, '[',''),']','') as domain
	,keyword_id
	,keyword
	,convert(date, created_at) as created_at
	,tags
	,search_type
	,search_volume
	,rank_id
	,convert(date, hist_date) as hist_date
	,convert(float, [rank]) as [rank]
	,convert(float, shareofvoice) as shareofvoice
	,convert(float, shareofvoicepercentage) as shareofvoicepercentage
	,highest_rank_comp_page
	,is_feature_snippet
	,has_video
	--,case
	--	when charindex('''id'': ', landing_page) = 0 then NULL
	--	else substring(landing_page, charindex('''id'': ', landing_page) + 6, charindex(',', landing_page) - (charindex('''id'': ', landing_page) + 6)) 
	--	end
	,landing_page as landing_page_id
	--,case
	--	when charindex('''title'':', title_description) = 0 then NULL
	--	else trim(replace(substring(title_description, charindex('''title'':', title_description) + 8, 1000),'''}',''))
	--	end
	,title_description as title_desc
from
	playpen.jm_accuranker_ext_allkeywords

print 'step 5.5 done'


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 5.75 - Add new current keyword performance from the external table into the base tables
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 5.75 - Add new current keyword performance from the external table into the base tables',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;


declare @maxdate date
set @maxdate = (select max(hist_date) from playpen.jm_accuranker_keywords)

truncate table playpen.jm_accuranker_keywords_most_recent
insert into playpen.jm_accuranker_keywords_most_recent
select 
	*
from 
	playpen.jm_accuranker_keywords
where
	hist_date = @maxdate

print 'step 5.75 done'


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 6.5 - Add new competitor keyword from the external table into the base tables
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 6.5 - Add new competitor keyword from the external table into the base tables',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;
							   

delete from playpen.jm_accuranker_keywords_competitors
where hist_date in(select distinct convert(date, hist_date) from playpen.jm_accuranker_ext_allkeywordsCompetitors)

insert into playpen.jm_accuranker_keywords_competitors
select
	replace(replace(domain, '[',''),']','') as domain
	,keyword_id
	,keyword
	,convert(date, created_at) as created_at
	,comp_rank_id
	,convert(date, hist_date) as hist_date
	,convert(float, [rank]) as [rank]
	,convert(float, shareofvoice) as shareofvoice
	,convert(float, shareofvoicepercentage) as shareofvoicepercentage
	,highest_rank_comp_page
	,competitor_id
	,competitor_domain
	,competitor_display_name
from
	playpen.jm_accuranker_ext_allkeywordsCompetitors

print 'step 6.5 done'


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 7.5 - Add new seach volume keyword from the external table into the base tables
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 7.5 - Add new seach volume keyword from the external table into the base tables',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;
							   

truncate table 	playpen.jm_accuranker_keywords_search_volume				   
insert into playpen.jm_accuranker_keywords_search_volume
select 
	replace(replace(domain, '[',''),']','') as domain
	,keyword_id
	,keyword
	,convert(date, created_at) as created_at
	,convert(float, search_volume) as search_volume
from
	playpen.jm_accuranker_ext_allkeywordsSearch_volume

print 'step 7.5 done'


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 8.5 - Add new keyword tags from the external table into the base tables
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 8.5 - Add new keyword tags from the external table into the base tables',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;


insert into playpen.jm_accuranker_keywords_tag_combo
select 
	replace(replace(domain, '[',''),']','') as domain
	,keyword_id
	,keyword
	,tags
from 
	playpen.jm_accuranker_ext_allkeywords_tags_combo
where
	concat(keyword_id, tags) not in(select distinct concat(keyword_id, tags) from playpen.jm_accuranker_keywords_tag_combo)

--select * from playpen.jm_accuranker_keywords_tag_combo order by keyword_id

print 'step 8.5 done'


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 9.5 - Refresh the max date we have in the sql tables
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 9.5 - Refresh the max date we have in the sql tables',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;


truncate table playpen.accuranker_max_dates
insert into playpen.accuranker_max_dates
select min(maxdates) as maxDate from
(
	select max(hist_date) as maxdates from playpen.jm_accuranker_domain_historic
	union all
	select max(hist_date) as maxdates from playpen.jm_accuranker_keywords
	union all
	select max(hist_date) as maxdates from playpen.jm_accuranker_landing_page
) as a

print 'step 9.5 done'


--------------------------------------------------------------------------------------------------------------------------------------
-- Log Complete
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG @Batch_No       = @Batch_No_PV,		
                          @Proc_Name      = @Proc_Name_PV,		
                          @Proc_Call      = @Proc_Call_PV,		
                          @Exe_Status     = 'COMPLETED',
                          @Error_Detail   = NULL;

END TRY

--------------------------------------------------------------------------------------------------------------------------------------
-- Capture Errors
--------------------------------------------------------------------------------------------------------------------------------------

BEGIN CATCH

SELECT @Error_Detail_PV = ERROR_MESSAGE();

EXECUTE MONITOR.P_JOB_LOG @Batch_No       = @Batch_No_PV,		
                          @Proc_Name      = @Proc_Name_PV,		
                          @Proc_Call      = @Proc_Call_PV,		
                          @Exe_Status     = 'ERROR',
                          @Error_Detail   = @Error_Detail_PV;

THROW

END CATCH
END

--------------------------------------------------------------------------------------------------------------------------------------
-- End of Procedure
--------------------------------------------------------------------------------------------------------------------------------------

GO

