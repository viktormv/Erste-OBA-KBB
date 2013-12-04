CREATE OR REPLACE PACKAGE REPGA_DETM_OBA_U_BETET_PKG IS
  -- %version   0.2   2013.01.03
  -- %author   Vincze Csaba
  -- %usage loading package for stage area
  -- %intab tablelist:  REPGA_STAM_OBA_U_BETET
  --
  -- %outtab tablelist: REPGA_DETM_OBA_U_BETET
  PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_DETM_OBA_U_BETET_PKG IS
  -- %version   0.2   2013.01.03
  -- %version   0.1   2012.10.10
  -- %author   Vincze Csaba

  -- Procedure for calculating and loading data into REPGA_DETM_OBA_U_BETET table
  -- %param p_sym_run_data: symbols running time <br>
  --                in form of date type
  -- %raises NO
  -- %dynasql YES
  -- %autonom NO
  -- %dbmsout NO
  ------------------------------------------------------------------------
  --   0.1 Vincze Csaba        Intitial
  --   0.2 Vincze Csaba        s_ oszlopok nem kellenek

   GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_U_BETET_PKG';

   GC_TARGETTABLE CONSTANT VARCHAR2(30):='REPGA_DETM_OBA_U_BETET';

   G_PROCESSED NUMBER := 0;

   G_RUNID NUMBER;

   G_COMMITPOINT NUMBER := 200000;

   G_BATCHSIZE NUMBER := 5000;

   ----------------------------------------
   PROCEDURE DELETE_TABLE(P_SYM_RUN_DATE DATE) IS
      C_ACCTION CONSTANT VARCHAR2(6) := 'DELETE';
   BEGIN
      REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
      REPGA_UTIL_PKG.DELETE_TABLE( GC_TARGETTABLE, P_SYM_RUN_DATE );
   END;
   ----------------------------------------
   PROCEDURE INSERT_TABLE(P_SYM_RUN_DATE DATE) IS
      C_ACCTION CONSTANT VARCHAR2(8) := 'INSERT';
      L_ROWNUMBER NUMBER := 0;
      L_INDEX     NUMBER := 0;
      L_LAST      NUMBER;

      TYPE T_SYM_RUN_DATE    IS TABLE OF REPGA_DETM_OBA_U_BETET.SYM_RUN_DATE%TYPE INDEX BY PLS_INTEGER;
      TYPE T_RNO             IS TABLE OF REPGA_DETM_OBA_U_BETET.RNO%TYPE INDEX BY PLS_INTEGER;
      TYPE T_CLIENT_NO       IS TABLE OF REPGA_DETM_OBA_U_BETET.CLIENT_NO%TYPE INDEX BY PLS_INTEGER;
      TYPE T_SZLA            IS TABLE OF REPGA_DETM_OBA_U_BETET.SZLA%TYPE INDEX BY PLS_INTEGER;
      TYPE T_UGYLET          IS TABLE OF REPGA_DETM_OBA_U_BETET.UGYLET%TYPE INDEX BY PLS_INTEGER;
      TYPE T_TIPUS           IS TABLE OF REPGA_DETM_OBA_U_BETET.TIPUS%TYPE INDEX BY PLS_INTEGER;
      TYPE T_BJOGCIM         IS TABLE OF REPGA_DETM_OBA_U_BETET.BJOGCIM%TYPE INDEX BY PLS_INTEGER;
--      TYPE T_S_EGYENLEG      IS TABLE OF REPGA_DETM_OBA_U_BETET.S_EGYENLEG%TYPE INDEX BY PLS_INTEGER;
--      TYPE T_S_KAMAT         IS TABLE OF REPGA_DETM_OBA_U_BETET.S_KAMAT%TYPE INDEX BY PLS_INTEGER;
--      TYPE T_S_FORRASADO     IS TABLE OF REPGA_DETM_OBA_U_BETET.S_FORRASADO%TYPE INDEX BY PLS_INTEGER;
--      TYPE T_S_ZAROLT        IS TABLE OF REPGA_DETM_OBA_U_BETET.S_ZAROLT%TYPE INDEX BY PLS_INTEGER;
      TYPE T_EGYENLEG        IS TABLE OF REPGA_DETM_OBA_U_BETET.EGYENLEG%TYPE INDEX BY PLS_INTEGER;
      TYPE T_KAMAT           IS TABLE OF REPGA_DETM_OBA_U_BETET.KAMAT%TYPE INDEX BY PLS_INTEGER;
      TYPE T_FORRASADO       IS TABLE OF REPGA_DETM_OBA_U_BETET.FORRASADO%TYPE INDEX BY PLS_INTEGER;
      TYPE T_ZAROLT          IS TABLE OF REPGA_DETM_OBA_U_BETET.ZAROLT%TYPE INDEX BY PLS_INTEGER;

    L_SYM_RUN_DATE    T_SYM_RUN_DATE;
    L_RNO             T_RNO;
    L_CLIENT_NO       T_CLIENT_NO;
    L_SZLA            T_SZLA;
    L_UGYLET          T_UGYLET;
    L_TIPUS           T_TIPUS;
    L_BJOGCIM         T_BJOGCIM;
--    L_S_EGYENLEG      T_S_EGYENLEG;
--    L_S_KAMAT         T_S_KAMAT;
--    L_S_FORRASADO     T_S_FORRASADO;
--    L_S_ZAROLT        T_S_ZAROLT;
    L_EGYENLEG        T_EGYENLEG;
    L_KAMAT           T_KAMAT;
    L_FORRASADO       T_FORRASADO;
    L_ZAROLT          T_ZAROLT;

      CURSOR HC IS(
                    SELECT SYM_RUN_DATE,
                           RNO,
                           CLIENT_NO,
                           SZLA,
                           UGYLET,
                           TIPUS,
                           BJOGCIM,
--                           TO_NUMBER(REPLACE(S_EGYENLEG,',','.')) EGYENLEG,
--                           TO_NUMBER(REPLACE(S_KAMAT,',','.')) KAMAT,
--                           TO_NUMBER(REPLACE(S_FORRASADO,',','.')) FORRASADO,
--                           TO_NUMBER(REPLACE(S_ZAROLT,',','.')) ZAROLT
                           TO_NUMBER(REPLACE(EGYENLEG,',','.')) EGYENLEG,
                           TO_NUMBER(REPLACE(KAMAT,',','.')) KAMAT,
                           TO_NUMBER(REPLACE(FORRASADO,',','.')) FORRASADO,
                           TO_NUMBER(REPLACE(ZAROLT,',','.')) ZAROLT
                      FROM REPGA_STAM_OBA_U_BETET
                     WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
                     );

   BEGIN
      REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
      OPEN HC;
      LOOP
         REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' ||
                                         TO_CHAR(L_ROWNUMBER));
         L_SYM_RUN_DATE.DELETE;
         FETCH HC BULK COLLECT
            INTO L_SYM_RUN_DATE,
             L_RNO,
             L_CLIENT_NO,
             L_SZLA,
             L_UGYLET,
             L_TIPUS,
             L_BJOGCIM,
--             L_S_EGYENLEG,
--             L_S_KAMAT,
--             L_S_FORRASADO,
--             L_S_ZAROLT,
             L_EGYENLEG,
             L_KAMAT,
             L_FORRASADO,
             L_ZAROLT
           LIMIT G_BATCHSIZE;
         L_LAST := L_SYM_RUN_DATE.COUNT;
         EXIT WHEN L_LAST = 0;
         L_ROWNUMBER := L_ROWNUMBER + L_LAST;

         FORALL X IN 1 .. L_LAST
            INSERT INTO REPGA_DETM_OBA_U_BETET
                (SYM_RUN_DATE,
                 RNO,
                 CLIENT_NO,
                 SZLA,
                 UGYLET,
                 TIPUS,
                 BJOGCIM,
--                 S_EGYENLEG,
--                 S_KAMAT,
--                 S_FORRASADO,
--                 S_ZAROLT,
                 EGYENLEG,
                 KAMAT,
                 FORRASADO,
                 ZAROLT,
                 RUN_ID)
            VALUES
              ( L_SYM_RUN_DATE(X),
                L_RNO(X),
                L_CLIENT_NO(X),
                L_SZLA(X),
                L_UGYLET(X),
                L_TIPUS(X),
                L_BJOGCIM(X),
--                L_S_EGYENLEG(X),
--                L_S_KAMAT(X),
--                L_S_FORRASADO(X),
--                L_S_ZAROLT(X),
                L_EGYENLEG(X),
                L_KAMAT(X),
                L_FORRASADO(X),
                L_ZAROLT(X),
                G_RUNID);
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
   PROCEDURE MOD_TABLE(P_SYM_RUN_DATE DATE) IS

   BEGIN

     INSERT INTO REPGA_TMPM_U_BETET(SELECT SYM_RUN_DATE,
                                           CLIENT_NO, SZLA, TIPUS,
                                           SUM(EGYENLEG) EGYENLEG,
                                           SUM(KAMAT) KAMAT,
                                           SUM(FORRASADO) FORRASADO,
                                           SUM(ZAROLT) ZAROLT
                                      FROM REPGA_DETM_OBA_U_BETET
                                     WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
                                       AND (SZLA, TIPUS) IN (SELECT SZLA, TIPUS
                                                               FROM REPGA_DETM_OBA_U_BETET
                                                              WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
                                                              GROUP BY SZLA, TIPUS
                                                             HAVING COUNT(1)>1
                                                            )
                                     GROUP BY SYM_RUN_DATE, CLIENT_NO, SZLA, TIPUS
                                   );

    DELETE FROM REPGA_DETM_OBA_U_BETET
          WHERE (SYM_RUN_DATE, CLIENT_NO, SZLA, TIPUS) IN (SELECT SYM_RUN_DATE, CLIENT_NO, SZLA, TIPUS
                                                             FROM REPGA_TMPM_U_BETET);

    INSERT INTO REPGA_DETM_OBA_U_BETET (SYM_RUN_DATE, CLIENT_NO, SZLA, TIPUS, BJOGCIM, EGYENLEG, KAMAT, FORRASADO, ZAROLT)
      SELECT SYM_RUN_DATE, CLIENT_NO, SZLA, TIPUS, 'ERSTE' BJOGCIM, EGYENLEG, KAMAT, FORRASADO, ZAROLT
        FROM REPGA_TMPM_U_BETET;
    COMMIT;

    INSERT INTO REPGA_TMPM_OBA_URBIS_MHT
       SELECT DISTINCT E.CLIENT_NO, SZL, TUL_H, B.CLIENT_NO OLD_CLIENT_NO
         FROM REPGA_DETM_OBA_U_CLIENT E, REPGA_DETM_OBA_U_BETET B
        WHERE E.SYM_RUN_DATE = B.SYM_RUN_DATE
          AND B.SZLA = E.SZL
          AND EXISTS  -- ELHUNYT SZLATULAJDONOS
              (
              SELECT 1
                FROM REPGA_DETM_OBA_U_CLIENT
               WHERE SYM_RUN_DATE = B.SYM_RUN_DATE
                 AND ELHAL_DT IS NOT NULL
                 AND CLIENT_NO = B.CLIENT_NO
              );

    DELETE FROM REPGA_TMPM_OBA_URBIS_MHT
          WHERE  SZL IN (SELECT SZL
                           FROM REPGA_TMPM_OBA_URBIS_MHT
                          GROUP BY SZL
                         HAVING SUM(TUL_H) > 100
                        );
    COMMIT;
    INSERT INTO REPGA_TMPM_OBA_URBIS_MHT
      SELECT (SELECT DISTINCT OLD_CLIENT_NO
                FROM   REPGA_TMPM_OBA_URBIS_MHT
               WHERE  SZL = X.SZL
             ) CLIENT_NO, SZL,
             100-SUM(TUL_H) TUL_H, NULL
        FROM REPGA_TMPM_OBA_URBIS_MHT X
       WHERE SZL IN (SELECT SZL
                       FROM   REPGA_TMPM_OBA_URBIS_MHT
                      GROUP BY SZL
                     HAVING SUM(TUL_H) < 99
                    )
       GROUP BY SZL;
    COMMIT;

      FOR rec IN (SELECT SZLA, CLIENT_NO,
                         SUM(EGYENLEG) EGYENLEG,
                         SUM(KAMAT) KAMAT,
                         SUM(FORRASADO) FORRASADO,
                         SUM(ZAROLT) ZAROLT
                    FROM (SELECT SZLA, X.CLIENT_NO, TUL_H,
                                 ROUND(TUL_H*EGYENLEG/100,0) EGYENLEG,
                                 ROUND(TUL_H*KAMAT/100,0) KAMAT,
                                 ROUND(TUL_H*FORRASADO/100,0) FORRASADO,
                                 ROUND(TUL_H*ZAROLT/100,0) ZAROLT
                            FROM REPGA_TMPM_OBA_URBIS_MHT X, REPGA_DETM_OBA_U_BETET B
                           WHERE B.SYM_RUN_DATE = P_SYM_RUN_DATE
                             AND SZLA = SZL
                        )
                    GROUP BY SZLA, CLIENT_NO
                 ) LOOP
      --
      UPDATE REPGA_DETM_OBA_U_BETET
         SET RNO = 0
       WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
         AND SZLA = REC.SZLA;
      --
      INSERT INTO REPGA_TMPM_OBA_U_BETET_NEW (CLIENT_NO, SZLA, UGYLET, TIPUS, BJOGCIM, EGYENLEG, KAMAT, FORRASADO, ZAROLT)
        SELECT REC.CLIENT_NO, SZLA, UGYLET, TIPUS,
               (SELECT NVL2(OLD_CLIENT_NO,'Öröklés miatti betét','ERSTE')
                  FROM REPGA_TMPM_OBA_URBIS_MHT
                 WHERE szl = rec.szla
                   AND CLIENT_NO = REC.CLIENT_NO
               ) BJOGCIM,
               REC.EGYENLEG, REC.KAMAT, REC.FORRASADO, REC.ZAROLT
          FROM REPGA_DETM_OBA_U_BETET
         WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
           AND SZLA = REC.SZLA;
      --
    END LOOP;
    --
    DELETE FROM REPGA_DETM_OBA_U_BETET
     WHERE RNO = 0;
    --
    INSERT INTO REPGA_DETM_OBA_U_BETET (SYM_RUN_DATE, CLIENT_NO, SZLA, UGYLET, TIPUS, BJOGCIM, EGYENLEG, KAMAT, FORRASADO, ZAROLT)
      SELECT P_SYM_RUN_DATE, CLIENT_NO, SZLA, UGYLET, TIPUS, BJOGCIM, EGYENLEG, KAMAT, FORRASADO, ZAROLT
        FROM REPGA_TMPM_OBA_U_BETET_NEW;
    --
    COMMIT;

   END;
   ----------------------------------------
   PROCEDURE MOD_CLIENT_TABLE(P_SYM_RUN_DATE DATE) IS
   BEGIN

    UPDATE REPGA_DETM_OBA_U_CLIENT
       SET RNO = 1
     WHERE SYM_RUN_DATE = P_SYM_RUN_DATE;
    COMMIT;

    FOR rec IN (SELECT CLIENT_NO, SZL
                  FROM (SELECT CLIENT_NO, SZL,
                               ROW_NUMBER () OVER (PARTITION BY CLIENT_NO
                                                   ORDER BY SZL DESC) R_NO
                          FROM REPGA_DETM_OBA_U_CLIENT
                         WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
                      )
                  WHERE  R_NO > 1
                  ) LOOP
                --
     UPDATE REPGA_DETM_OBA_U_CLIENT
        SET RNO = NULL
      WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
        AND CLIENT_NO = REC.CLIENT_NO
        AND SZL = REC.SZL;
                --
    END LOOP;
    COMMIT;

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
      G_RUNID := REPGA_BACKOFFICE_PKG.START_API(P_APPNAME      => GC_APPNAME,
                                                P_SYM_RUN_DATE => P_SYM_RUN_DATE);
      G_PROCESSED :=0;
      PREPROCESS(P_SYM_RUN_DATE);
      DELETE_TABLE(P_SYM_RUN_DATE);
      INSERT_TABLE(P_SYM_RUN_DATE);
      MOD_TABLE(P_SYM_RUN_DATE);
      MOD_CLIENT_TABLE(P_SYM_RUN_DATE);
      POSTPROCESS(P_SYM_RUN_DATE);
      REPGA_BACKOFFICE_PKG.END_API(G_PROCESSED);
   EXCEPTION
      WHEN OTHERS THEN
         REPGA_BACKOFFICE_PKG.END_ERROR_API(SQLCODE,SQLERRM);
   END;

END;
/
