SELECT MACHINE_ID, ROUND(AVG(END_TIME - START_TIME), 3) AS processing_time FROM   
    (
        SELECT A.MACHINE_ID,B.TIMESTAMP as END_TIME,A.TIMESTAMP AS START_TIME 
        FROM ACTIVITY A
        INNER JOIN ACTIVITY B
        ON A.MACHINE_ID=B.MACHINE_ID AND A.PROCESS_ID=B.PROCESS_ID
    AND A.ACTIVITY_TYPE='start' AND B.ACTIVITY_TYPE='end'
    )Z
GROUP BY MACHINE_ID
;
