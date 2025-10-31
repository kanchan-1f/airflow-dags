app_queries = {
    # "Installs": """
    #     SELECT COUNT(AppsFlyer_ID) AS Install
    #     FROM `analytics-1f.AppsflyerDB.Installs-v2`
    #     WHERE DATE(Install_Time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    # """,
    "Registration": """
        SELECT COUNT(customer_ID) AS Registration
        FROM `MasterDB.1_source_t-1`
        WHERE Registration_Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
          AND System_Status = 'Live'
          AND Internal_Test_Tagging IS NULL
    """,
    "Same Day Registration": """
        WITH Install_CTE AS (
            SELECT 
                DATE(Install_Time) AS Date,
                COUNT(AppsFlyer_ID) AS Install_Count
            FROM `analytics-1f.AppsflyerDB.Installs-v2`
            GROUP BY DATE(Install_Time)
        ),
        SameDay_CTE AS (
            SELECT 
                DATE(install_date) AS Date,
                COUNT(appsflyer_id) AS SameDay_Registration
            FROM `MasterDB.1_source_t-1`
            WHERE DATE(install_date) = Registration_Date
            AND System_Status = 'Live'
            AND Internal_Test_Tagging IS NULL
            GROUP BY DATE(install_date)
        )
        SELECT 
            CASE 
                WHEN COALESCE(i.Install_Count, 0) > 0 THEN 
                    CONCAT(ROUND(COALESCE(s.SameDay_Registration, 0) / COALESCE(i.Install_Count, 0) * 100, 0), '%')
                ELSE '0%'
            END AS SameDay_Registration_Percent
        FROM 
            Install_CTE i
        FULL OUTER JOIN 
            SameDay_CTE s ON i.Date = s.Date
        WHERE 
            COALESCE(i.Date, s.Date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
            """,

    "Total Registration Percentage": """
        WITH Install_CTE AS (
            SELECT 
                DATE(Install_Time) AS Date,
                COUNT(AppsFlyer_ID) AS Install_Count
            FROM `analytics-1f.AppsflyerDB.Installs-v2`
            GROUP BY DATE(Install_Time)
        ),


        Registration_CTE AS (
            SELECT 
                Registration_Date AS Date,
                COUNT(customer_ID) AS Total_Registration
            FROM `MasterDB.1_source_t-1`
            WHERE System_Status = 'Live'
            AND Internal_Test_Tagging IS NULL
            GROUP BY Registration_Date
        )
        SELECT 
            CASE 
                WHEN COALESCE(i.Install_Count, 0) > 0 THEN 
                    CONCAT(ROUND(COALESCE(r.Total_Registration, 0) / COALESCE(i.Install_Count, 0) * 100, 0), '%')
                ELSE '0%'
            END AS Total_Registration_Percent
        FROM 
            Install_CTE i
        FULL OUTER JOIN 
            Registration_CTE r ON i.Date = r.Date
        WHERE 
            COALESCE(i.Date, r.Date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);    
    """,
        "MoneySign": """
            WITH cleaned_mobile AS (
                SELECT
                    CASE
                        WHEN mobile_number LIKE '%Deleted%' THEN REGEXP_REPLACE(mobile_number, 'Deleted', '')
                        ELSE mobile_number
                    END AS Mobile,
                    *
                FROM `MasterDB.1_source_t-1`
                WHERE System_Status = 'Live'
                AND Internal_Test_Tagging IS NULL
            )
            SELECT COUNT(cm.customer_ID)
            FROM `analytics-1f.MoneySignDB.ms_master` AS master
            LEFT JOIN cleaned_mobile AS cm
            ON CAST(master.mobile AS STRING) = cm.Mobile
            WHERE master.MoneySignGeneratedDate IS NOT NULL
            AND cm.mobile_number IS NOT NULL
            AND DATE(master.MoneySignGeneratedDate) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    """,
    "RIA Signed": """
        SELECT COUNT(customer_ID) AS RIA_Signed
        FROM `MasterDB.1_source_t-1`
        WHERE RIA_Signed_Date IS NOT NULL
          AND RIA_Signed_Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
          AND System_Status = 'Live'
          AND Internal_Test_Tagging IS NULL
    """,
    "Latest Version Upgrades (Inception)": """
        -- Inception
        SELECT COUNT(DISTINCT customer_ID)
        FROM `MasterDB.1_source_t-1`
        WHERE Internal_Test_Tagging IS NULL
          AND System_Status = 'Live'
          AND app_version = '2.4.0'
          AND install_date >= '2024-04-29'
          AND appsflyer_id NOT IN (
              SELECT DISTINCT appsflyer_ID
              FROM `AppsflyerDB.actual_uninstalls`
              WHERE appsflyer_ID IS NOT NULL
          )
          """,
      "Latest Version Upgrades t-1": """
        -- Registered user who have upgraded
        SELECT COUNT(DISTINCT customer_ID)
        FROM `MasterDB.1_source_t-1`
        WHERE Internal_Test_Tagging IS NULL
          AND System_Status = 'Live'
          AND app_version = '2.4.0'
          AND install_date < '2024-04-29'
          AND appsflyer_id NOT IN (
              SELECT DISTINCT appsflyer_ID
              FROM `AppsflyerDB.actual_uninstalls`
              WHERE appsflyer_ID IS NOT NULL
          )
    """,
    "Uninstalls": """
        SELECT COUNT(Customer_User_ID) AS Uninstall
        FROM `analytics-1f.AppsflyerDB.actual_uninstalls`
        WHERE DATE(Event_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    """}
    # "Execution": """
    #     -- Provide your query here
    # """,
    
meeting_queries = {
    "Meeting Booked": """
    select Count(*) from `MasterDB.1_source_t-1` where Discovery_Booking_Date = date_sub(current_date(), interval 1 Day)
    and System_Status = 'Live' and Internal_Test_Tagging is null
    """,

    "Discovery Scheduled": """
    select Count(customer_ID) as Discovery_Schedule from `MasterDB.1_source_t-1` 
    where Discovery_Appointment_Date = date_sub(current_date(), interval 1 Day)
    and System_Status = 'Live' and Internal_Test_Tagging is null
    """,

    "Discovery Completed": """
    select count(customer_ID) as Discovery_Completed from `MasterDB.1_source_t-1` where Discovery_Appointment_Date = date_sub(current_date(), interval 1 Day)
    and Discovery_Meeting_Status = 'Completed' 
    and System_Status = 'Live' and Internal_Test_Tagging is null
    """,

    "Same day%": """
        SELECT 
        CONCAT(
            ROUND(
            100.0 * COUNT(customer_ID) / 
            (SELECT COUNT(customer_ID) 
            FROM `MasterDB.1_source_t-1` 
            WHERE Registration_Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            AND System_Status = 'Live' 
            AND Internal_Test_Tagging IS NULL), 0), '%') AS Same_Day_Registration_Percentage
        FROM `MasterDB.1_source_t-1`
        WHERE Discovery_Booking_Date = Registration_Date
        AND Registration_Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND System_Status = 'Live'
        AND Internal_Test_Tagging IS NULL;
    """,
    "Spilover%": """
    WITH TotalRegistrations AS (
    SELECT customer_ID
    FROM `MasterDB.1_source_t-1`
    WHERE Registration_Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND System_Status = 'Live'
        AND Internal_Test_Tagging IS NULL
    ),
    SameDayRegistrations AS (
    SELECT customer_ID
    FROM `MasterDB.1_source_t-1`
    WHERE Discovery_Booking_Date = Registration_Date
        AND Registration_Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND System_Status = 'Live'
        AND Internal_Test_Tagging IS NULL
    )
    SELECT 
    CONCAT(
        ROUND(100.0 * (COUNT(total.customer_ID) - COUNT(same_day.customer_ID)) / COUNT(total.customer_ID), 0), '%'
    ) AS Spillover_Percentage
    FROM TotalRegistrations total
    LEFT JOIN SameDayRegistrations same_day
    ON total.customer_ID = same_day.customer_ID;
    """,
    "No Show%": """
        WITH TotalAppointments AS (
        SELECT COUNT(customer_ID) AS Total_Count
        FROM `MasterDB.1_source_t-1`
        WHERE Discovery_Appointment_Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            AND System_Status = 'Live'
            AND Internal_Test_Tagging IS NULL
        ),
        NoShows AS (
        SELECT COUNT(customer_ID) AS No_Show_Count
        FROM `MasterDB.1_source_t-1`
        WHERE Discovery_Appointment_Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            AND Discovery_Meeting_Status IN ('Not Attended', 'Not attended', 'No Show')
            AND System_Status = 'Live'
            AND Internal_Test_Tagging IS NULL
        )
        SELECT 
        ROUND(100.0 * NoShows.No_Show_Count / TotalAppointments.Total_Count, 2) AS No_Show_Percentage
        FROM NoShows, TotalAppointments;

    """,
    "Cancellation%": """
        WITH TotalAppointments AS (
        SELECT COUNT(customer_ID) AS Total_Count
        FROM `MasterDB.1_source_t-1`
        WHERE Discovery_Appointment_Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            AND System_Status = 'Live'
            AND Internal_Test_Tagging IS NULL
        ),
        CancelledMeetings AS (
        SELECT COUNT(customer_ID) AS Cancelled_Count
        FROM `MasterDB.1_source_t-1`
        WHERE Discovery_Appointment_Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            AND Meeting_Remark = 'Cancelled'
            AND System_Status = 'Live'
            AND Internal_Test_Tagging IS NULL
        )
        SELECT 
        ROUND(100.0 * CancelledMeetings.Cancelled_Count / TotalAppointments.Total_Count, 2) AS Meeting_Cancelled_Percentage
        FROM CancelledMeetings, TotalAppointments;

    """,
    "Consultation Booked": """
        SELECT COUNT(customer_ID) AS Consultation_Booked
        FROM `MasterDB.1_source_t-1`
        WHERE FWP_Appointment_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
          AND System_Status = 'Live'
          AND Internal_Test_Tagging IS NULL
    """,
    "Consultation Completed": """
        SELECT COUNT(customer_ID) AS Consultation_Completed
        FROM `MasterDB.1_source_t-1`
        WHERE FWP_Appointment_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
          AND FWP_Meeting_Status = 'Completed'
          AND System_Status = 'Live'
          AND Internal_Test_Tagging IS NULL
    """,
    "Quaterly Booked": """
        SELECT COUNT(customer_ID) AS Quartely_Booked
        FROM `MasterDB.1_source_t-1`
        WHERE Quaterly_Meeting_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
          AND System_Status = 'Live'
          AND Internal_Test_Tagging IS NULL
    """,
    "Quaterly Completed": """
        SELECT COUNT(customer_ID) AS Quartely_Completed
        FROM `MasterDB.1_source_t-1`
        WHERE Quaterly_Meeting_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
          AND Quaterly_Meeting_Status = 'Completed'
          AND System_Status = 'Live'
          AND Internal_Test_Tagging IS NULL
    """,
    "Payments": """
        WITH cte AS (
            SELECT Payment.Payment_Date AS Transaction_date,
                   Payment.c_key,
                   Payment.transaction_id,
                   Payment.user_code,
                   member_id AS RIA_Member_ID,
                   Pm.Customer_name,
                   Internal_Test_Tagging,
                   Payment.amount,
                   Payment.source,
                   Payment.invoice_doc_link,
                   Payment.description,
                   Payment.transaction_type,
                   Payment.payment_status,
                   Current_Cycle,
                   status AS is_refund,
                   DATE_ADD(FWP_Sent_Date, INTERVAL 85 DAY) AS Eligible_Date
            FROM (
                SELECT DATE(TIMESTAMP(created_at), "Asia/Kolkata") AS Payment_Date,
                       CASE
                           WHEN source LIKE '%Phonepe%' THEN merchant_transaction_id
                           ELSE transaction_id
                       END AS c_key,
                       user_code,
                       transaction_id,
                       amount,
                       source,
                       invoice_doc_link,
                       description,
                       transaction_type,
                       payment_status,
                       NULL AS status
                FROM `EosDB.payment_transactions`
                WHERE is_active
                  AND is_success
                  AND transaction_type = 'Credit'
                  AND transaction_id IS NOT NULL
                  AND payment_status = 'COMPLETED'
                  AND amount > 0
                UNION ALL
                SELECT DATE(TIMESTAMP(created_at), "Asia/Kolkata") AS Payment_Date,
                       CASE
                           WHEN source LIKE '%Phonepe%' AND merchant_transaction_id LIKE '%REF%' THEN REGEXP_EXTRACT(merchant_transaction_id, r'[^-]-([^ ])')
                           ELSE transaction_id
                       END AS c_key,
                       user_code,
                       transaction_id,
                       amount,
                       source,
                       invoice_doc_link,
                       description,
                       transaction_type,
                       payment_status,
                       'Yes' AS status
                FROM `EosDB.payment_transactions`
                WHERE is_active
                  AND is_success
                  AND transaction_type = 'Debit'
                  AND transaction_id IS NOT NULL
                  AND payment_status = 'COMPLETED'
                  AND amount > 0
            ) AS Payment
            LEFT JOIN `analytics-1f.MasterDB.1_source` AS mb ON Payment.user_code = mb.customer_ID
            LEFT JOIN `MasterDB.Payment_master` AS pm ON Payment.user_code = pm.Customer_id
            WHERE Internal_Test_Tagging IS NULL
              AND member_id NOT IN ('1F2400799', '1FAC230105', '1FAB230103')
              AND status IS NULL
              AND pm.is_refund = FALSE
              AND Payment.Payment_Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        ),
        cte2 AS (
            SELECT DISTINCT customer_ID AS ID, mobile_number AS mn
            FROM `MasterDB.Payment_master`
            WHERE payment_status = 'COMPLETED'
              AND Test_internal IS NULL
              AND is_refund = FALSE
        )
        SELECT COUNT(*)
        FROM cte
        LEFT JOIN cte2 ON cte.user_code = cte2.ID
        WHERE cte2.mn NOT IN (
            SELECT CAST(Mobile_Number AS STRING)
            FROM `AppsflyerDB.new_test_user`
            WHERE Mobile_Number IS NOT NULL
        )
    """,
    "VUA": """
        SELECT ROUND(SUM(VUA)) AS VUA
        FROM `MasterDB.1_source_t-1`
        WHERE FWP_Ready_Date IS NOT NULL
          AND System_Status = 'Live'
          AND Internal_Test_Tagging IS NULL
    """,
    "Avg VUA": """
        SELECT ROUND(AVG(VUA)) AS VUA
        FROM `MasterDB.1_source_t-1`
        WHERE FWP_Ready_Date IS NOT NULL
          AND System_Status = 'Live'
          AND Internal_Test_Tagging IS NULL
    """,
    "Avg Income": """
        SELECT ROUND(AVG(income_lac)) AS Avg_income_lac
        FROM `MasterDB.1_source_t-1`
        WHERE System_Status = 'Live'
          AND Internal_Test_Tagging IS NULL
    """,
    "Avg Age": """
        SELECT ROUND(AVG(age)) AS Avg_age
        FROM `MasterDB.1_source_t-1`
        WHERE System_Status = 'Live'
          AND Internal_Test_Tagging IS NULL
    """
}



krp_installs = {
    "Installs": """
        SELECT COUNT(AppsFlyer_ID) AS Install
        FROM `analytics-1f.AppsflyerDB.Installs-v2`
        WHERE DATE(Install_Time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    """,

}

    
# android_installs = """
#     WITH totalInstalls AS (
#     SELECT COUNT(Install_Time) AS total_installs 
#     FROM  `analytics-1f.AppsflyerDB.Installs-v2`
#     WHERE  DATE(Install_Time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)),
#     AndroidInstalls AS (
#     SELECT COUNT(Install_Time) AS android_installs 
#     FROM `analytics-1f.AppsflyerDB.Installs-v2`
#     WHERE DATE(Install_Time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
#     AND Platform = 'android')
#     SELECT  CONCAT(ROUND((a.android_installs / t.total_installs) * 100), '%') AS android_install_percentage
#     FROM TotalInstalls t
#     JOIN AndroidInstalls a ON 1=1
#     """
    
# ios_installs =  """
#     WITH TotalInstalls AS (
#     SELECT COUNT(Install_Time) AS total_installs 
#     FROM `analytics-1f.AppsflyerDB.Installs-v2`
#     WHERE DATE(Install_Time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)),
#     iOSInstalls AS (
#     SELECT COUNT(Install_Time) AS ios_installs 
#     FROM `analytics-1f.AppsflyerDB.Installs-v2`
#     WHERE DATE(Install_Time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND Platform = 'ios')
#     SELECT CONCAT(ROUND((i.ios_installs / t.total_installs) * 100), '%') AS ios_install_percentage
#     FROM TotalInstalls t
#     JOIN iOSInstalls i ON 1=1
#     """


 



krp_registration = {
    
    "Registration": """
    SELECT COUNT(customer_ID) AS Registration_Count
    FROM `MasterDB.1_source_t-1`
    WHERE Registration_Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND System_Status = 'Live'
    AND Internal_Test_Tagging IS NULL;
    """,

    # "Same Day Registration": """
    #     WITH Install_CTE AS (
    #         SELECT 
    #             DATE(Install_Time) AS Date,
    #             COUNT(AppsFlyer_ID) AS Install_Count
    #         FROM `analytics-1f.AppsflyerDB.Installs-v2`
    #         GROUP BY DATE(Install_Time)
    #     ),
    #     SameDay_CTE AS (
    #         SELECT 
    #             DATE(install_date) AS Date,
    #             COUNT(appsflyer_id) AS SameDay_Registration
    #         FROM `MasterDB.1_source_t-1`
    #         WHERE DATE(install_date) = Registration_Date
    #         AND System_Status = 'Live'
    #         AND Internal_Test_Tagging IS NULL
    #         GROUP BY DATE(install_date)
    #     )
    #     SELECT 
    #         CASE 
    #             WHEN COALESCE(i.Install_Count, 0) > 0 THEN 
    #                 CONCAT(ROUND(COALESCE(s.SameDay_Registration, 0) / COALESCE(i.Install_Count, 0) * 100, 0), '%')
    #             ELSE '0%'
    #         END AS SameDay_Registration_Percent
    #     FROM 
    #         Install_CTE i
    #     FULL OUTER JOIN 
    #         SameDay_CTE s ON i.Date = s.Date
    #     WHERE 
    #         COALESCE(i.Date, s.Date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
    #         """,

    # "Total Registration Percentage": """
    #     WITH Install_CTE AS (
    #         SELECT 
    #             DATE(Install_Time) AS Date,
    #             COUNT(AppsFlyer_ID) AS Install_Count
    #         FROM `analytics-1f.AppsflyerDB.Installs-v2`
    #         GROUP BY DATE(Install_Time)
    #     ),


    #     Registration_CTE AS (
    #         SELECT 
    #             Registration_Date AS Date,
    #             COUNT(customer_ID) AS Total_Registration
    #         FROM `MasterDB.1_source_t-1`
    #         WHERE System_Status = 'Live'
    #         AND Internal_Test_Tagging IS NULL
    #         GROUP BY Registration_Date
    #     )
    #     SELECT 
    #         CASE 
    #             WHEN COALESCE(i.Install_Count, 0) > 0 THEN 
    #                 CONCAT(ROUND(COALESCE(r.Total_Registration, 0) / COALESCE(i.Install_Count, 0) * 100, 0), '%')
    #             ELSE '0%'
    #         END AS Total_Registration_Percent
    #     FROM 
    #         Install_CTE i
    #     FULL OUTER JOIN 
    #         Registration_CTE r ON i.Date = r.Date
    #     WHERE 
    #         COALESCE(i.Date, r.Date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);     """
    }

krp_money_sign = {
    "MoneySign": """
    with cleaned_mobile as (
      select
        case
          when mobile_number like '%Deleted%' then REGEXP_REPLACE(mobile_number, 'Deleted', '')
          else mobile_number end as Mobile,* from `MasterDB.1_source_t-1` where System_Status = 'Live' and Internal_Test_Tagging is null)
    
    select Count(cm.customer_ID)
    from `analytics-1f.MoneySignDB.ms_master` as master
    left join 
      cleaned_mobile as cm
    on
    cast(master.mobile AS STRING) = cm.Mobile
    where master.MoneySignGeneratedDate is not null and cm.mobile_number is not null 
    and date(master.MoneySignGeneratedDate) = date_sub(current_date(), interval 1 Day)
    """}

krp_meeting = {
    "Meeting Booked": """
    select Count(*) from `MasterDB.1_source_t-1` where Discovery_Booking_Date = date_sub(current_date(), interval 1 Day)
    and System_Status = 'Live' and Internal_Test_Tagging is null
    """,

    "Discovery Scheduled": """
    select Count(customer_ID) as Discovery_Schedule from `MasterDB.1_source_t-1` 
    where Discovery_Appointment_Date = date_sub(current_date(), interval 1 Day)
    and System_Status = 'Live' and Internal_Test_Tagging is null
    """,

    "Discovery Completed": """
    select count(customer_ID) as Discovery_Completed from `MasterDB.1_source_t-1` where Discovery_Appointment_Date = date_sub(current_date(), interval 1 Day)
    and Discovery_Meeting_Status = 'Completed' 
    and System_Status = 'Live' and Internal_Test_Tagging is null
    """,

    "RIA Signed": """
    select count(customer_ID) as RIA_Signed from `MasterDB.1_source_t-1` where RIA_Signed_Date is not null
    and RIA_Signed_Date = date_sub(current_date(), interval 1 Day)
    and System_Status = 'Live' and Internal_Test_Tagging is null
    """,

    "FWP Consultation Scheduled": """
    select count(customer_ID) as Consultation_Booked from `MasterDB.1_source_t-1` where FWP_Appointment_date = date_sub(current_date(), interval 1 Day)
    and System_Status = 'Live' and Internal_Test_Tagging is null
    """,

    "FWP Consultation Completed": """
    select count(customer_ID) as Consultation_Completed from `MasterDB.1_source_t-1` where FWP_Appointment_date = date_sub(current_date(), interval 1 Day)
    and FWP_Meeting_Status = 'Completed' 
    and System_Status = 'Live' and Internal_Test_Tagging is null
    """,

    "Quarterly Scheduled": """
    with cte AS (
        select distinct(Latest_Quaterly_Meeting_Status),count(customer_ID) from `MasterDB.1_source_t-1` 
        where date(Latest_Quaterly_Meeting_date) = date_sub(current_date(), interval 1 Day)
        and Internal_Test_Tagging is null and System_Status = 'Live'
        group by 1 
        order by 1
        )
    Select Count(*) FROM cte
    """,

    "Quarterly Completed": """
    with cte AS (select distinct(Latest_Quaterly_Meeting_Status),count(customer_ID) from `MasterDB.1_source_t-1` 
    where date(Latest_Quaterly_Meeting_date) = date_sub(current_date(), interval 1 Day)
    and Latest_Quaterly_Meeting_Status = 'Completed'
    and Internal_Test_Tagging is null and System_Status = 'Live'
    group by 1 
    order by 1)
    Select Count(*) FROM cte
    """,


    # "VUA Added": """
    # SELECT CAST(ROUND(SUM(VUA)) + 
    # (SELECT ROUND(SUM(VUA)) 
    # FROM `MasterDB.1_source_t-1` 
    # WHERE FWP_Ready_Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    # AND FWP_Ready_Date IS NOT NULL 
    # AND System_Status = 'Live' 
    # AND Internal_Test_Tagging IS NULL) AS INT) -
    # CAST(ROUND(SUM(VUA)) AS INT) AS VUA_Added
    # FROM `MasterDB.1_source_t-1` 
    # WHERE FWP_Ready_Date <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    # AND FWP_Ready_Date IS NOT NULL 
    # AND System_Status = 'Live' 
    # AND Internal_Test_Tagging IS NULL;
    # """,
}
krp_uninstall= {"Uninstalls": """
    select Count(Customer_User_ID) as Uninstall from `analytics-1f.AppsflyerDB.actual_uninstalls` 
    where Date(Event_date) = date_sub(current_date(), interval 1 Day)
    """}
