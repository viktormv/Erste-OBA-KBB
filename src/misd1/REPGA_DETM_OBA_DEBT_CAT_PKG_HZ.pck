CREATE OR REPLACE PACKAGE REPGA_DETM_OBA_DEBT_CAT_PKG_HZ IS
  -- %version   0.1   2013.08.27
  -- %author   Hivekovics Zoltán
  -- %usage loading package for stage area
  -- %intab tablelist:  REPGA_DETM_OBA_TBASE, REPGA_DETM_OBA_RES_B, REPGA_DETM_OBA_RES_U, MI_RB_ACCT_MTH, MI_RB_TRAN_HIST_MTH, REPGA_STAM_CL_BAL
  --
  -- %outtab tablelist: REPGA_DETM_OBA_DEBT_CAT

   PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_DETM_OBA_DEBT_CAT_PKG_HZ IS
  -- %version   0.1   2013.08.27 HZ initial version
  -- %author   Hivekovics Zoltán

  -- Procedure for calculating and loading data into REPGA_DETM_OBA_DEBT_CAT table
  -- %param p_sym_run_data: symbols running time <br>
  --                in form of date type
  -- %raises NO
  -- %dynasql YES
  -- %autonom NO
  -- %dbmsout NO
   GC_APPNAME              CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_DEBT_CAT_PKG';
   GC_TARGETTABLE          CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_DEBT_CAT';

   G_PROCESSED                      NUMBER := 0;
   G_RUNID                          NUMBER;
   G_COMMITPOINT                    NUMBER := 200000;
   G_BATCHSIZE                      NUMBER := 5000;

   GC_STABILITY_CODE_STEADY       CONSTANT VARCHAR2(1) := 'S';
   GC_STABILITY_CODE_LIGHT_STEADY CONSTANT VARCHAR2(1) := 'L';

   ----------------------------------------
   PROCEDURE DELETE_TABLE(P_SYM_RUN_DATE DATE) IS
      C_ACCTION CONSTANT VARCHAR2(6) := 'DELETE';
   BEGIN
      --REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
      REPGA_UTIL_PKG.DELETE_TABLE( GC_TARGETTABLE, P_SYM_RUN_DATE );
   END;
   ----------------------------------------
   PROCEDURE INSERT_TABLE(P_SYM_RUN_DATE DATE) IS
      C_ACCTION   CONSTANT VARCHAR2(8) := 'INSERT';
      L_ROWNUMBER NUMBER := 0;
      L_INDEX     NUMBER := 0;
      L_LAST      NUMBER;

      TYPE T_CLIENT_NO            IS TABLE OF REPGA_DETM_OBA_DEBT_CAT.CLIENT_NO%TYPE INDEX BY PLS_INTEGER;
      TYPE T_ACCOUNT_NO           IS TABLE OF REPGA_DETM_OBA_DEBT_CAT.ACCOUNT_NO%TYPE INDEX BY PLS_INTEGER;
      TYPE T_DEBT_TYPE            IS TABLE OF REPGA_DETM_OBA_DEBT_CAT.DEBT_TYPE%TYPE INDEX BY PLS_INTEGER;
      TYPE T_BAL_HUF              IS TABLE OF REPGA_DETM_OBA_DEBT_CAT.BAL_HUF%TYPE INDEX BY PLS_INTEGER;
      TYPE T_OBA_LIMIT            IS TABLE OF REPGA_DETM_OBA_DEBT_CAT.OBA_LIMIT%TYPE INDEX BY PLS_INTEGER;
      TYPE T_DEBT_SUM             IS TABLE OF REPGA_DETM_OBA_DEBT_CAT.DEBT_SUM%TYPE INDEX BY PLS_INTEGER;
      TYPE T_DEBT_RATE            IS TABLE OF REPGA_DETM_OBA_DEBT_CAT.DEBT_RATE%TYPE INDEX BY PLS_INTEGER;
      TYPE T_BAL_HUF_PART         IS TABLE OF REPGA_DETM_OBA_DEBT_CAT.BAL_HUF_PART%TYPE INDEX BY PLS_INTEGER;
      TYPE T_DEBT_STABILITY_FLAG  IS TABLE OF REPGA_DETM_OBA_DEBT_CAT.DEBT_STABILITY_FLAG%TYPE INDEX BY PLS_INTEGER;
      TYPE T_ACCOUNT_FLAG         IS TABLE OF REPGA_DETM_OBA_DEBT_CAT.ACCOUNT_FLAG%TYPE INDEX BY PLS_INTEGER;
      TYPE T_TRANS_FLAG           IS TABLE OF REPGA_DETM_OBA_DEBT_CAT.TRANS_FLAG%TYPE INDEX BY PLS_INTEGER;
      TYPE T_CREDIT_FLAG          IS TABLE OF REPGA_DETM_OBA_DEBT_CAT.CREDIT_FLAG%TYPE INDEX BY PLS_INTEGER;

      L_CLIENT_NO                 T_CLIENT_NO;
      L_ACCOUNT_NO                T_ACCOUNT_NO;
      L_DEBT_TYPE                 T_DEBT_TYPE;
      L_BAL_HUF                   T_BAL_HUF;
      L_OBA_LIMIT                 T_OBA_LIMIT;
      L_DEBT_SUM                  T_DEBT_SUM;
      L_DEBT_RATE                 T_DEBT_RATE;
      L_BAL_HUF_PART              T_BAL_HUF_PART;
      L_DEBT_STABILITY_FLAG       T_DEBT_STABILITY_FLAG;
      L_ACCOUNT_FLAG              T_ACCOUNT_FLAG;
      L_TRANS_FLAG                T_TRANS_FLAG;
      L_CREDIT_FLAG               T_CREDIT_FLAG;

      L_SYM_RUN_DATE              DATE;

      CURSOR HC IS
      WITH TMP_CLIENT_DEBT AS (SELECT /*+MATERIALIZE*/
                                      SRC.CLIENT_NO,
                                      MAX(SRC.OBA_LIMIT)                                         OBA_LIMIT,
                                      SUM(SRC.DEBT_SUM)                                          DEBT_SUM,
                                     (CASE WHEN SUM(SRC.ACCOUNT_CNT) >= 1 THEN 'Y' ELSE 'N' END) ACCOUNT_FLAG,
                                     (CASE WHEN SUM(SRC.TRANS_CNT)   >= 1 THEN 'Y' ELSE 'N' END) TRANS_FLAG,
                                     (CASE WHEN SUM(SRC.CREDIT_CNT)  >= 1 THEN 'Y' ELSE 'N' END) CREDIT_FLAG
                               FROM (--BETET
                                     SELECT CLIENT.CLIENT_NO,
                                            CLIENT.OBA_LIMIT,
                                            ACCOUNT_BET.DEBT_SUM,
                                            ACCOUNT_BET.DEBT_CNT,
                                            ACCOUNT_BET.ACCOUNT_CNT,
                                            0 TRANS_CNT,
                                            0 CREDIT_CNT
                                     FROM (SELECT ORB.NEW_CSOP_ID,
                                                  SUM (CASE WHEN ORB.TIPUS = 'F' THEN 1 ELSE 0 END)           ACCOUNT_CNT,
                                                  SUM (CASE WHEN ORB.TIPUS = 'B' THEN 1 ELSE 0 END)           DEBT_CNT,
                                                  SUM (CASE WHEN ORB.TIPUS = 'B' THEN ORB.BAL_HUF ELSE 0 END) DEBT_SUM
                                           FROM REPGA_DETM_OBA_RES_B@MISINTEG.ERSTE.HU ORB
                                           WHERE     ORB.SYM_RUN_DATE = P_SYM_RUN_DATE
                                           GROUP BY ORB.NEW_CSOP_ID) ACCOUNT_BET,
                                          (SELECT ORU.NEW_CSOP_ID,
                                                  OB.AZONOSITO CLIENT_NO,
                                                  ORU.OBA_LIMIT
                                           FROM REPGA_DETM_OBA_RES_U@MISINTEG.ERSTE.HU ORU,
                                                REPGA_DETM_OBA_TBASE@MISINTEG.ERSTE.HU OB
                                           WHERE    ORU.SYM_RUN_DATE = P_SYM_RUN_DATE
                                                AND OB.SYM_RUN_DATE  = P_SYM_RUN_DATE
                                                AND OB.NEW_CSOP_ID   = ORU.NEW_CSOP_ID
                                                ) CLIENT
                                     WHERE CLIENT.NEW_CSOP_ID = ACCOUNT_BET.NEW_CSOP_ID
                                     ---------------------------
                                     UNION ALL
                                     ---------------------------
                                     --MUNKABER ATUTALAS
                                     SELECT AM.CLIENT_NO,
                                            0 OBA_LIMIT,
                                            0 DEBT_SUM,
                                            0 DEBT_CNT,
                                            0 ACCOUNT_CNT,
                                            COUNT(1) TRANS_CNT,
                                            0 CREDIT_CNT
                                     FROM MI_RB_ACCT_MTH@MISINTEG.ERSTE.HU AM,
                                          MI_RB_TRAN_HIST_MTH@MISINTEG.ERSTE.HU THM
                                     WHERE     AM.INTERNAL_KEY    = THM.INTERNAL_KEY
                                          AND AM.SYM_RUN_DATE     = P_SYM_RUN_DATE
                                          AND THM.SYM_RUN_DATE    = P_SYM_RUN_DATE
                                          AND THM.CR_DR_MAINT_IND = 'C'
                                          AND THM.TRAN_TYPE      IN ('B007', 'B107')
                                     GROUP BY CLIENT_NO
                                    ---------------------------
                                    UNION ALL
                                    ---------------------------
                                    --HITEL
                                    SELECT CB.CLIENT_NO,
                                           0 OBA_LIMIT,
                                           0 DEBT_SUM,
                                           0 DEBT_CNT,
                                           0 ACCOUNT_CNT,
                                           0 TRANS_CNT,
                                           COUNT(1) CREDIT_CNT
                                    FROM REPGA_STAM_CL_BAL@MISINTEG.ERSTE.HU CB
                                    WHERE     CB.SYM_RUN_DATE = TO_DATE('2010.12.31','YYYY.MM.DD') --P_SYM_RUN_DATE
                                         AND CB.SAP_MATURITY IN ('3','4','5')
                                         AND CB.SAP_FOKONYV  IN ('H342100', 'H345100')
                                    GROUP BY CB.CLIENT_NO
                                    ) SRC
                                 GROUP BY SRC.CLIENT_NO
                                 HAVING SUM(SRC.DEBT_CNT) >= 1
                                 )
      --STABIL betet megbontas
      SELECT BASE.AZONOSITO CLIENT_NO,
             BET.ACCOUNT_NO,
             BASE.TIPUS DEBT_TYPE,
             BET.BAL_HUF,
             T.OBA_LIMIT,
             T.DEBT_SUM,
             (CASE WHEN T.DEBT_SUM > T.OBA_LIMIT AND T.OBA_LIMIT != 0 THEN (T.OBA_LIMIT /T.DEBT_SUM)
                   WHEN T.OBA_LIMIT = 0 THEN 0
                   ELSE 1
                   END ) DEBT_RATE,
             (CASE WHEN T.DEBT_SUM > T.OBA_LIMIT AND T.OBA_LIMIT != 0 THEN (T.OBA_LIMIT /T.DEBT_SUM) * BET.BAL_HUF
                   WHEN T.OBA_LIMIT = 0 THEN 0
                   ELSE BET.BAL_HUF
                   END ) BAL_HUF_PART,
             GC_STABILITY_CODE_STEADY DEBT_STABILITY_FLAG,
             T.ACCOUNT_FLAG,
             T.TRANS_FLAG,
             T.CREDIT_FLAG
      FROM REPGA_DETM_OBA_RES_B@MISINTEG.ERSTE.HU BET,
           REPGA_DETM_OBA_TBASE@MISINTEG.ERSTE.HU BASE,
           TMP_CLIENT_DEBT T
      WHERE     BET.SYM_RUN_DATE  = P_SYM_RUN_DATE
            AND BASE.SYM_RUN_DATE = P_SYM_RUN_DATE
            AND BET.NEW_CSOP_ID   = BASE.NEW_CSOP_ID
            --AND BASE.TIPUS IN ('M','E','T') -- Minden típus kell
            AND BET.TIPUS = 'B'
            AND T.CLIENT_NO = BASE.AZONOSITO
            AND (   T.ACCOUNT_FLAG = 'Y'
                 OR T.TRANS_FLAG   = 'Y'
                 OR T.CREDIT_FLAG  = 'Y' )
      UNION ALL
      -- NEM STABIL betet megbontas
      SELECT BASE.AZONOSITO CLIENT_NO,
             BET.ACCOUNT_NO,
             BASE.TIPUS DEBT_TYPE,
             BET.BAL_HUF,
             T.OBA_LIMIT,
             T.DEBT_SUM,
             (CASE WHEN T.ACCOUNT_FLAG = 'N' AND T.TRANS_FLAG = 'N' AND T.CREDIT_FLAG = 'N' THEN 1
                   ELSE (1 - (T.OBA_LIMIT /T.DEBT_SUM))
                   END ) DEBT_RATE,
             (CASE WHEN T.ACCOUNT_FLAG = 'N' AND T.TRANS_FLAG = 'N' AND T.CREDIT_FLAG = 'N' THEN BET.BAL_HUF
                   ELSE ((1 - (T.OBA_LIMIT /T.DEBT_SUM)) * BET.BAL_HUF )
                   END ) BAL_HUF_PART,
             GC_STABILITY_CODE_LIGHT_STEADY DEBT_STABILITY_FLAG,
             T.ACCOUNT_FLAG,
             T.TRANS_FLAG,
             T.CREDIT_FLAG
      FROM REPGA_DETM_OBA_RES_B@MISINTEG.ERSTE.HU BET,
           REPGA_DETM_OBA_TBASE@MISINTEG.ERSTE.HU BASE,
           TMP_CLIENT_DEBT T
      WHERE     BET.SYM_RUN_DATE  = P_SYM_RUN_DATE
            AND BASE.SYM_RUN_DATE = P_SYM_RUN_DATE
            AND BET.NEW_CSOP_ID   = BASE.NEW_CSOP_ID
            --AND BASE.TIPUS IN ('M','E','T') -- Minden típus kell
            AND BET.TIPUS = 'B'
            AND T.CLIENT_NO = BASE.AZONOSITO
            AND ( (T.DEBT_SUM > T.OBA_LIMIT)
                   OR
                  (    T.ACCOUNT_FLAG = 'N'
                   AND T.TRANS_FLAG   = 'N'
                   AND T.CREDIT_FLAG  = 'N') );

   BEGIN
      IF (P_SYM_RUN_DATE = TRUNC(P_SYM_RUN_DATE+1, 'MM')-1) THEN
         L_SYM_RUN_DATE := P_SYM_RUN_DATE;
      ELSE
         L_SYM_RUN_DATE := TRUNC(P_SYM_RUN_DATE, 'MM')-1;
      END IF;
      --REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
      OPEN HC;
      LOOP
         --REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' || TO_CHAR(L_ROWNUMBER));
         L_CLIENT_NO.DELETE;
         FETCH HC
             BULK COLLECT INTO L_CLIENT_NO,
                               L_ACCOUNT_NO,
                               L_DEBT_TYPE,
                               L_BAL_HUF,
                               L_OBA_LIMIT,
                               L_DEBT_SUM,
                               L_DEBT_RATE,
                               L_BAL_HUF_PART,
                               L_DEBT_STABILITY_FLAG,
                               L_ACCOUNT_FLAG,
                               L_TRANS_FLAG,
                               L_CREDIT_FLAG

         LIMIT G_BATCHSIZE;
         L_LAST := L_CLIENT_NO.COUNT;
         EXIT WHEN L_LAST = 0;
         L_ROWNUMBER := L_ROWNUMBER + L_LAST;

         FORALL X IN 1 .. L_LAST
            INSERT INTO REPGA_DETM_OBA_DEBT_CAT
               (SYM_RUN_DATE,
                RUN_ID,
                CLIENT_NO,
                ACCOUNT_NO,
                DEBT_TYPE,
                BAL_HUF,
                OBA_LIMIT,
                DEBT_SUM,
                DEBT_RATE,
                BAL_HUF_PART,
                DEBT_STABILITY_FLAG,
                ACCOUNT_FLAG,
                TRANS_FLAG,
                CREDIT_FLAG)
            VALUES
               (L_SYM_RUN_DATE,
                G_RUNID,
                L_CLIENT_NO(X),
                L_ACCOUNT_NO(X),
                L_DEBT_TYPE(X),
                L_BAL_HUF(X),
                L_OBA_LIMIT(X),
                L_DEBT_SUM(X),
                L_DEBT_RATE(X),
                L_BAL_HUF_PART(X),
                L_DEBT_STABILITY_FLAG(X),
                L_ACCOUNT_FLAG(X),
                L_TRANS_FLAG(X),
                L_CREDIT_FLAG(X));
         IF L_INDEX >= G_COMMITPOINT THEN
            COMMIT;
            L_INDEX := 0;
         ELSE
            L_INDEX := L_INDEX + L_LAST;
         END IF;
      END LOOP;
      COMMIT;
      CLOSE HC;
      G_PROCESSED:= G_PROCESSED+L_ROWNUMBER;
   END;

   ----------------------------------------
   PROCEDURE PREPROCESS(P_SYM_RUN_DATE DATE) IS
      C_ACCTION CONSTANT VARCHAR2(10) := 'PREPROCESS';
   BEGIN
      REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
      G_BATCHSIZE   := REPGA_UTIL_PKG.GET_BATCHSIZE(GC_TARGETTABLE,  G_BATCHSIZE);
      G_COMMITPOINT := REPGA_UTIL_PKG.GET_COMMITPOINT(GC_TARGETTABLE,G_COMMITPOINT);
      REPGA_UTIL_PKG.CLEANUP(P_SYM_RUN_DATE => P_SYM_RUN_DATE,
                             P_APPNAME      => GC_APPNAME,
                             P_TARGETNAME   => GC_TARGETTABLE);
   END;
   ----------------------------------------
   PROCEDURE POSTPROCESS(P_SYM_RUN_DATE DATE) IS
      C_ACCTION CONSTANT VARCHAR2(11) := 'POSTPROCESS';
   BEGIN
      REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
      REPGA_UTIL_PKG.HOUSE_KEEPING(P_SYM_RUN_DATE => P_SYM_RUN_DATE,
                                   P_APPNAME      => GC_APPNAME,
                                   P_TARGETNAME   => GC_TARGETTABLE);
   END;
   ----------------------------------------
   PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE) IS
   BEGIN
      G_RUNID := -1;/*REPGA_BACKOFFICE_PKG.START_API(P_APPNAME      => GC_APPNAME,
                                                P_SYM_RUN_DATE => P_SYM_RUN_DATE);*/
      G_PROCESSED :=0;
--      PREPROCESS(P_SYM_RUN_DATE);
      DELETE_TABLE(P_SYM_RUN_DATE);
      INSERT_TABLE(P_SYM_RUN_DATE);
/*      POSTPROCESS(P_SYM_RUN_DATE);
      REPGA_BACKOFFICE_PKG.END_API(G_PROCESSED);
   EXCEPTION
      WHEN OTHERS THEN
         REPGA_BACKOFFICE_PKG.END_ERROR_API(SQLCODE,SQLERRM);*/
   END;

END;
/
