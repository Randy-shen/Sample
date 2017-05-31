
CREATE OR REPLACE FUNCTION bi_dw.f_dealership_make_history_daily(_dateid date DEFAULT (('now'::text)::date - 1))
 RETURNS void
 LANGUAGE plpgsql
AS $function$ 
declare
	results int4;

BEGIN

	
	--select current_Date-1 into start_date;
	--select max(date_id) from bi_dw.f_dealership_make_history_daily into start_date;
	
	
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
		-- 1. Report Start Date Active Dealers
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ 
	RAISE NOTICE 'Start - Step 1 Report Start Date Active Dealers on %',$1;
	
	delete from bi_dw.f_dealership_make_history_daily where date_id = $1;

	
	GET DIAGNOSTICS results = ROW_COUNT;
	RAISE NOTICE '		1.0 - Delete % rows on % from f_dealership_make_history_daily',results,$1;
    
	-- Insert active dealers
	
	INSERT INTO bi_dw.f_dealership_make_history_daily
	(dealership_id, make_id, date_id, status_id, activity_date, deactivation_reason, pre_deactivation_reason)
	SELECT DISTINCT
			rad.dealership_id,
			rad.make_id,
			$1,
			case when coalesce(p.parent_dealership_id,-1) NOT IN (17156,7440,5178) then 6 else 4 end, -- Active Dealers - Start Date, cancellation for moving to mbusa or volve
			null::timestamp,
			'',
			NULL -- pre deactivation reason
	FROM 
		bi_dw.d_dealership_make rad
		   LEFT JOIN bi_dw.d_dealership_parent P
		ON (p.create_date::date-1)<= $1-1 AND (p.expire_date::date-1) > $1-1
		and p.dealership_id = rad.dealership_id
	WHERE
			coalesce(p.parent_dealership_id,-1) NOT IN (17156,7440,5178)  -- 17156 Volvo cars of north america, 7440 MBUSA, 5178 zag test auto group
			and lower(rad.dealership_status) IN ('active','active (issues)')
			and rad.create_date <= $1-1
			and rad.expire_date > $1-1;
			
	GET DIAGNOSTICS results = ROW_COUNT;
	
	RAISE NOTICE '		1.1 - insert % Active Dealers at the beginning of %',results,$1;
	RAISE NOTICE 'End - Step 1 Report Start Date Active Dealers on %',$1;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	
		-- 2. New Dealers Added
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	RAISE NOTICE 'Start - Step 2 Report New Dealers Added';
	
	INSERT INTO bi_dw.f_dealership_make_history_daily
	(dealership_id, make_id, date_id, status_id, activity_date, deactivation_reason, pre_deactivation_reason)
	SELECT DISTINCT
			rad.dealership_id,
			rad.make_id,
			$1,
			2, -- Newly Activated
			$1,
			coalesce(t3.deactivation_reason,''),
			NULL -- pre deactivation reason
		FROM
			bi_dw.d_dealership_make rad
		LEFT JOIN bi_dw.d_dealership_parent p
		ON (p.create_date::date-1) <= $1 AND (p.expire_date::date-1) > $1
		and p.dealership_id= rad.dealership_id
	    LEFT JOIN bi_dw.d_dealership_deactivation_reason t3
			ON rad.dealership_id = t3.dealership_id
			AND $1 >= (t3.create_date::date - 1)
			AND $1 <= (t3.expire_date::date - 1)
		LEFT JOIN 
		(
			SELECT DISTINCT
				dealership_id,
				make_id
			FROM
				bi_dw.d_dealership_make
			WHERE
				expire_date <= $1
				AND lower(dealership_status) IN ('active','active (issues)')
		) i_rad
			ON rad.dealership_id = i_rad.dealership_id
			AND rad.make_id = i_rad.make_id
		WHERE
		    coalesce(p.parent_dealership_id,-1) NOT IN (17156,7440,5178) 
			AND lower(rad.dealership_status) IN ('active','active (issues)')
			AND rad.create_date = $1
			AND i_rad.dealership_id IS null;
	
	GET DIAGNOSTICS results = ROW_COUNT;
	
	RAISE NOTICE '		2.0 - insert % new dealers for %',results,$1;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	
		-- 2.1 New dealers added by moving to non MBUSA group
		-- remove the duplicate from the first new dealer query
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	--RAISE NOTICE 'Step 2.1 Report New Dealers Added - moving out of MBUSA';	
	
	INSERT INTO bi_dw.f_dealership_make_history_daily
	(dealership_id, make_id, date_id, status_id, activity_date, deactivation_reason, pre_deactivation_reason)
	    SELECT 
			FACT.dealership_id,
			FACT.make_id,
			FACT.date_id,
			FACT.status_id,
			FACT.activity_date,
			FACT.deactivation_reason,
			NULL -- pre deactivation reason
		FROM (
		SELECT DISTINCT
			RAD.dealership_id,
			RAD.make_id,
			$1 AS date_id,
			CASE WHEN EXISTS(SELECT 1 FROM bi_dw.f_dealership_make_history_daily A WHERE status_id =1 AND date_id < $1 AND A.dealership_id = RAD.dealership_id and A.make_id=RAD.make_id) THEN 3 ELSE 2 end as status_id, -- Newly Activated, -- Newly Activated
			$1 AS activity_Date,
			'DLR - Change in Dealer Group (MBUSA)' AS deactivation_reason-- Change in dealer group
		FROM
			bi_dw.d_dealership_make RAD
		LEFT JOIN bi_dw.d_dealership_parent P
		ON (P.create_date::date-1) <= $1 AND (P.expire_date::date-1) > $1
		and P.dealership_id = RAD.dealership_id
		JOIN 
		(
			SELECT DISTINCT
				REL.dealership_id,
				REL.make_id
			FROM
				bi_dw.d_dealership_make REL
			LEFT JOIN bi_dw.d_dealership_parent P
		ON (P.create_date::date-1) <= $1-1 AND (P.expire_date::date-1) > $1-1
		and P.dealership_id = REL.dealership_id
			WHERE
			REL.create_date::date <= $1 -1
			AND REL.expire_date::date > $1 -1
			and coalesce(P.parent_dealership_id,-1) IN (17156,7440,5178) 
			--AND DEALERSHIP_STATUS_ID IN (1,2)
		) I_RAD
			ON RAD.dealership_id = I_RAD.dealership_id
			AND RAD.make_id = I_RAD.make_id
		LEFT OUTER JOIN bi_dw.f_dealership_make_history_daily FDN
			ON FDN.dealership_id = RAD.dealership_id
			and FDN.make_id = RAD.make_id
			AND FDN.date_id = $1
		WHERE
		    coalesce(P.parent_dealership_id,-1) not in (17156,7440,5178) 
			AND lower(RAD.dealership_status) in ('active','active (issues)')
			AND RAD.create_date::date <= $1
			AND RAD.expire_date::date > $1
		) FACT
		LEFT JOIN bi_dw.f_dealership_make_history_daily NEW1
		ON FACT.dealership_id = NEW1.dealership_id
		and FACT.make_id = NEW1.make_id
		AND FACT.date_id = NEW1.date_id
		AND FACT.status_id = NEW1.status_id
		WHERE NEW1.dealership_id is null ;
		
	GET DIAGNOSTICS results = ROW_COUNT;
	
	RAISE NOTICE '		2.1 - insert % new dealers for % (moving out of MBUSA)',results,$1;
	RAISE NOTICE 'End - Step 2 Report New Dealers Added';
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	
	--3. Cancelled Dealers Reactivated
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	RAISE NOTICE 'Start - Step 3 Cancelled Dealers Reactivated';	
	
	INSERT INTO bi_dw.f_dealership_make_history_daily
	(dealership_id, make_id, date_id, status_id, activity_date, deactivation_reason, pre_deactivation_reason)
		SELECT DISTINCT
			RAD.dealership_id,
			RAD.make_id,
			$1 ,
			3, -- Reactivate
			$1,
			coalesce(T3.deactivation_reason,'') as deactivation_reason,
			NULL -- pre deactivation reason
		FROM
			bi_dw.d_dealership_make RAD 
		JOIN 
		(
			SELECT DISTINCT
				dealership_id,
				make_id
			FROM
				bi_dw.d_dealership_make
			WHERE
				expire_date <= $1
				AND lower(dealership_status) in ('active','active (issues)')
		) I_RAD
			ON RAD.dealership_id = I_RAD.dealership_id
			AND RAD.make_id = I_RAD.make_id
		JOIN 
		(
			SELECT DISTINCT
				dealership_id,
				make_id
			FROM
				bi_dw.d_dealership_make
			WHERE
				expire_date <= $1
				AND lower(dealership_status) in ('deleted from salesforce','inactive (dealership no longer on the program)','pending','suspended')-- from active to pending counted as inactive
		) I_RAD2
			ON RAD.dealership_id = I_RAD2.dealership_id
			AND RAD.make_id= I_RAD2.make_id
	    LEFT JOIN bi_dw.d_dealership_deactivation_reason T3
			ON RAD.dealership_id = T3.dealership_id
			AND $1 >= (T3.create_date::date - 1)
			AND $1 <= (T3.expire_date::date - 1)
		LEFT JOIN bi_dw.d_dealership_parent P
		ON (P.create_date::date-1) <= $1 AND (P.expire_date::date-1) > $1
		and P.dealership_id = RAD.dealership_id
		WHERE
			coalesce(P.parent_dealership_id,-1) not in (17156,7440,5178) 
			AND lower(RAD.dealership_status) in ('active','active (issues)')
			AND RAD.create_date = $1;
		
	GET DIAGNOSTICS results = ROW_COUNT;
	
	RAISE NOTICE '		insert % cancelled dealers reactivated for % ',results,$1;
	RAISE NOTICE 'End - Step 3 Cancelled Dealers Reactivated';
	

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	
	-- 4. Current Dealers Cancelled
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	RAISE NOTICE 'Start - Step 4 Current Dealers Cancelled';	
	
	INSERT INTO bi_dw.f_dealership_make_history_daily
	(dealership_id, make_id, date_id, status_id, activity_date, deactivation_reason, pre_deactivation_reason)
		SELECT DISTINCT
			RAD.dealership_id,
			RAD.make_id,
			$1 AS date_id,
			4 AS status_id,
			$1 AS A,
			coalesce(T3.deactivation_reason,'') as deactivation_reason,
			NULL -- pre deactivation reason
	
		FROM 
			bi_dw.d_dealership_make RAD 
		JOIN 
		(
			SELECT DISTINCT
				M.DEALERSHIP_ID,
				M.MAKE_ID
			FROM
				bi_dw.d_dealership_make M 
			LEFT JOIN bi_dw.d_dealership_parent P
			ON (P.create_date::date-1) <= $1-1 AND (P.expire_date::date-1) > $1-1
				and P.dealership_id = M.dealership_id
			WHERE
				M.expire_date = $1
				AND lower(M.dealership_status) in ('active','active (issues)')
				AND coalesce(P.parent_dealership_id,-1) not in (17156,7440,5178)  -- DEALER cancelled and moved to mbusa at same day
		) I_RAD
			ON RAD.dealership_id = I_RAD.dealership_id
			AND RAD.make_id= I_RAD.make_id
	    LEFT JOIN bi_dw.d_dealership_deactivation_reason T3
			ON RAD.dealership_id = T3.dealership_id
			AND $1 >= T3.create_date::date - 1
			AND $1 <= T3.expire_date::date - 1
		WHERE
		 RAD.create_date = $1
			AND lower(RAD.dealership_status) in ('deleted from salesforce','inactive (dealership no longer on the program)');
	
	GET DIAGNOSTICS results = ROW_COUNT;
	
	RAISE NOTICE '		4.0 - insert % cancelled dealers for % ',results,$1;		
	
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	
	-- 4.1 Inactive to pending
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	--RAISE NOTICE '	Step 4.1 Inactive to pending';	
	
	INSERT INTO bi_dw.f_dealership_make_history_daily
	(dealership_id, make_id, date_id, status_id, activity_date, deactivation_reason, pre_deactivation_reason)
		SELECT DISTINCT
			RAD.dealership_id,
			RAD.make_id,
			$1 AS date_id,
			4 AS status_id,
			$1 AS A,
			coalesce(T3.deactivation_reason,'') as deactivation_reason,
			NULL -- pre deactivation reason
	
		FROM 
			bi_dw.d_dealership_make RAD
			
		JOIN 
		(
			SELECT DISTINCT
				M.dealership_id,
				M.make_id
			FROM
				bi_dw.d_dealership_make M 
			LEFT JOIN bi_dw.d_dealership_parent P
			ON (P.create_date::date-1) <= $1-1 AND (P.expire_date::date-1) > $1-1
				and M.dealership_id = P.dealership_id
			WHERE
				M.expire_date = $1
				and lower(M.dealership_status) in ('active','active (issues)')
				and coalesce(P.parent_dealership_id,-1) not in (17156,7440,5178) 
			
		) I_RAD
			ON RAD.dealership_id = I_RAD.dealership_id
			AND RAD.make_id = I_RAD.make_id
	    LEFT JOIN bi_dw.d_dealership_deactivation_reason T3
			ON RAD.dealership_id = T3.dealership_id
			AND $1 >= T3.create_date::date - 1
			AND $1 <= T3.expire_date::date - 1
		LEFT JOIN bi_dw.d_dealership_parent P
		ON (P.create_date::date-1) <= $1 AND (P.expire_date::date-1) > $1
		and P.dealership_id = RAD.dealership_id
		WHERE
			coalesce(P.parent_dealership_id,-1) not in (17156,7440,5178) 
			and RAD.create_date = $1
			and lower(RAD.dealership_status) IN ('pending','pending (dealership pricing)','pending (dms & dealer pricing)','pending (dms)','pending (pricing dept.)');
	
	GET DIAGNOSTICS results = ROW_COUNT;
	
	RAISE NOTICE '		4.1 - insert % cancelled dealers for % (Inactive to pending)',results,$1;		
		
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	
	-- 4.2 cancelled dealers which moved to MBUSA
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	--RAISE NOTICE '	4.2 cancelled dealers which moved to MBUSA';	
	
	INSERT INTO bi_dw.f_dealership_make_history_daily
	(dealership_id, make_id, date_id, status_id, activity_date, deactivation_reason, pre_deactivation_reason)
		SELECT DISTINCT
			RAD.dealership_id,
			RAD.make_id,
			$1 ,
			4, 
			$1,
			'DLR - Change in Dealer Group (MBUSA)',
			NULL -- pre deactivation reason
		FROM
			bi_dw.d_dealership_make RAD
		LEFT JOIN bi_dw.d_dealership_parent P
		ON (P.create_date::date-1) <= $1 AND (P.expire_date::date-1) > $1
		and P.dealership_id = RAD.dealership_id
		JOIN 
		(
			SELECT DISTINCT
				REL.DEALERSHIP_ID,
				MAKE_ID
			FROM
				bi_dw.d_dealership_make REL
			LEFT JOIN bi_dw.d_dealership_parent P
		ON (P.create_date::date-1) <= $1-1 AND (P.expire_date::date-1) > $1-1
		and P.dealership_id = REL.dealership_id
			WHERE
			REL.create_date <= $1-1
			AND REL.expire_date > $1 -1
			AND coalesce(P.parent_dealership_id,-1) not in (17156,7440,5178) 
			AND lower(REL.dealership_status) in ('active','active (issues)')--NOT IN (4,12,7)
		) I_RAD
			ON RAD.dealership_id = I_RAD.dealership_id
			AND RAD.make_id = I_RAD.make_id
		WHERE
		    coalesce(P.parent_dealership_id,-1) in (17156,7440,5178) 
			AND lower(RAD.dealership_status) in ('active','active (issues)')
			AND RAD.create_date <= $1
			AND RAD.expire_date > $1;
		
	GET DIAGNOSTICS results = ROW_COUNT;
	
	RAISE NOTICE '		4.2 - insert % cancelled dealers for % (moved to MBUSA)',results,$1;	
	RAISE NOTICE 'End - Step 4 Current Dealers Cancelled';
	
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	
	-- 5. Current Dealers Suspended
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	RAISE NOTICE 'Start - Step 5 Current Dealers Suspended';	
	
	INSERT INTO bi_dw.f_dealership_make_history_daily
	(dealership_id, make_id, date_id, status_id, activity_date, deactivation_reason, pre_deactivation_reason)
		SELECT DISTINCT
			RAD.dealership_id,
			RAD.make_id,
			$1 AS date_id,
			7 AS status_id,
			$1 AS A,
			coalesce(T3.deactivation_reason,'') as deactivation_reason,
			NULL -- pre deactivation reason
	
		FROM 
			bi_dw.d_dealership_make RAD
		JOIN 
		(
			SELECT DISTINCT
				M.DEALERSHIP_ID,
				M.MAKE_ID
			FROM
				bi_dw.d_dealership_make M
			LEFT JOIN bi_dw.d_dealership_parent P
			ON P.create_date::date-1 <= $1-1 AND P.expire_date::date-1 > $1-1
				and M.dealership_id = P.dealership_id
			WHERE
				M.expire_date = $1
				AND lower(M.dealership_status) in ('active','active (issues)')
				AND coalesce(P.parent_dealership_id,-1) not in (17156,7440,5178) 
		) I_RAD
			ON RAD.dealership_id = I_RAD.dealership_id
			AND RAD.make_id= I_RAD.make_id

	    LEFT JOIN bi_dw.d_dealership_deactivation_reason T3
			ON RAD.dealership_id = T3.dealership_id
			AND $1 >= T3.create_date::date - 1
			AND $1 <= T3.expire_date::date - 1
		LEFT JOIN bi_dw.d_dealership_parent P
		ON P.create_date::date-1 <= $1 AND P.expire_date::date-1 > $1
		and P.dealership_id = RAD.dealership_id
		WHERE
			coalesce(P.parent_dealership_id,-1) not in (17156,7440,5178) 
			AND RAD.create_date = $1
			AND lower(RAD.dealership_status) IN ('suspended');
	
	GET DIAGNOSTICS results = ROW_COUNT;
	
	RAISE NOTICE '		insert % suspended dealers for %',results,$1;	
	RAISE NOTICE 'End - Step 5 Current Dealers Suspended';
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	
	-- 6. Report End Date Active Dealers
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	RAISE NOTICE 'Start - Step 6 Report End Date Active Dealers';	
	
	INSERT INTO bi_dw.f_dealership_make_history_daily
	(dealership_id, make_id, date_id, status_id, activity_date, deactivation_reason, pre_deactivation_reason)
		SELECT DISTINCT
			RAD.dealership_id,
			RAD.make_id,
			$1,
			1,
			null::timestamp,
			coalesce(T3.deactivation_reason,'') as deactivation_reason,
			NULL -- pre deactivation reason
		FROM 
			bi_dw.d_dealership_make RAD 
	    LEFT JOIN bi_dw.d_dealership_deactivation_reason T3
			ON RAD.dealership_id = T3.dealership_id
			AND $1 >= T3.create_date::date - 1
			AND $1 <= T3.expire_date::date - 1
		LEFT JOIN bi_dw.d_dealership_parent P
		ON P.create_date::date-1 <= $1 AND P.expire_date::date-1 > $1
		and P.dealership_id = RAD.dealership_id
		WHERE
			coalesce(P.parent_dealership_id,-1) not in (17156,7440,5178) 
			AND 
			lower(RAD.dealership_status) in ('active','active (issues)')
			AND RAD.expire_date > $1
			AND RAD.create_date <= $1;

	GET DIAGNOSTICS results = ROW_COUNT;
	
	RAISE NOTICE '		insert % active dealers at the end of %',results,$1;	
	RAISE NOTICE 'End - Step 6 Report End Date Active Dealers';
	
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	
	-- 7. Report Pending Dealers
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	RAISE NOTICE 'Start - Step 7 Report Pending Dealers';	
	
	INSERT INTO bi_dw.f_dealership_make_history_daily
	(dealership_id, make_id, date_id, status_id, activity_date, deactivation_reason, pre_deactivation_reason)
		SELECT
			B2.dealership_id,
			B2.make_id,
			$1,
			5,
			MIN(B2.create_date),
			coalesce(T3.deactivation_reason,'') as deactivation_reason,
			NULL -- pre deactivation reason
		FROM
			bi_dw.d_dealership_make B2 
	    LEFT JOIN bi_dw.d_dealership_deactivation_reason T3
			ON B2.dealership_id= T3.dealership_id
			AND $1 >= T3.create_date::date - 1
			AND $1 <= T3.expire_date::date - 1
		LEFT JOIN bi_dw.d_dealership_parent P
		ON P.create_date::date-1 <= $1 AND P.expire_date::date-1 > $1
		and P.dealership_id = B2.dealership_id
		WHERE
			coalesce(P.parent_dealership_id,-1) not in (17156,7440,5178) 
			and B2.create_date <= $1
			--AND B1.DEALERSHIP_ID IS NULL
			AND lower(B2.dealership_status) IN ('pending','pending (dealership pricing)','pending (dms & dealer pricing)','pending (dms)','pending (pricing dept.)')
			AND B2.expire_date > $1
		GROUP BY
			B2.dealership_id,
			B2.make_id,
			coalesce(T3.deactivation_reason,'');
	
	GET DIAGNOSTICS results = ROW_COUNT;
	
	RAISE NOTICE '		insert % pending dealers for %',results,$1;	
	RAISE NOTICE 'End - Step 7 Report Pending Dealers';	

		
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	
	-- 8. Update previous deactivation reason
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	
	update bi_dw.f_dealership_make_history_daily
	set pre_deactivation_reason = bi_dw.f_pre_deactivation_reason(dealership_id,make_id,date_id,status_id) 
	where status_id =3 and pre_deactivation_reason is null;
	
	
	update bi_dw.f_dealership_make_history_daily
	set pre_deactivation_reason = ''
	where pre_deactivation_reason is null;
	
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	
	-- 9. Update Redshift table
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	truncate table 	bi_dw.agg_dealership_make_history_daily;
	INSERT INTO bi_dw.agg_dealership_make_history_daily
(dealership_id, make_id, date_id, status_id, activity_date, deactivation_reason, pre_deactivation_reason)
	select 
dealership_id, make_id, date_id, status_id, activity_date, deactivation_reason, pre_deactivation_reason
	from bi_dw.f_dealership_make_history_daily
	where date_id =$1;
	
end;
$function$
