CREATE OR REPLACE PACKAGE REPGA_DETM_OBA_QCONTC_PKG IS
  -- %version   0.1   2012.10.10
  -- %author   Vincze Csaba
  -- %usage loading package for DETAIL area
  -- %intab tablelist:  REPGA_STAM_OBA_QCONTC
  --
  -- %outtab tablelist: REPGA_DETM_OBA_QCONTC
   PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_DETM_OBA_QCONTC_PKG IS
  -- %version   0.1   2012.10.10
  -- %author   Vincze Csaba

  -- Procedure for calculating and loading data into REPGA_DETM_OBA_QCONTC table
  -- %param p_sym_run_data: symbols running time <br>
  --                in form of date type
  -- %raises NO
  -- %dynasql YES
  -- %autonom NO
  -- %dbmsout NO
  ------------------------------------------------------------------------
  --   0.1 Vincze Csaba        Intitial

   GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_QCONTC_PKG';

   GC_TARGETTABLE CONSTANT VARCHAR2(30):='REPGA_DETM_OBA_QCONTC';

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

--      TYPE T_SYM_RUN_DATE     IS TABLE OF REPGA_DETM_OBA_QCONTC.SYM_RUN_DATE%TYPE INDEX BY PLS_INTEGER;
      TYPE T_CLIENT_NO   IS TABLE OF REPGA_DETM_OBA_QCONTC.CLIENT_NO%TYPE INDEX BY PLS_INTEGER;
      TYPE T_RUN_ID      IS TABLE OF REPGA_DETM_OBA_QCONTC.RUN_ID%TYPE INDEX BY PLS_INTEGER;

      L_CLIENT_NO  T_CLIENT_NO;
      L_RUN_ID     T_RUN_ID;

      CURSOR HC IS(
                    SELECT CLIENT_NO, RUN_ID
                      FROM REPGA_STAM_OBA_QCONTC
                     WHERE SYM_RUN_DATE = P_SYM_RUN_DATE
                     );
   BEGIN
      REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
      OPEN HC;
      LOOP
         REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' ||
                                         TO_CHAR(L_ROWNUMBER));
         L_CLIENT_NO.DELETE;
         FETCH HC BULK COLLECT
           INTO L_CLIENT_NO,
                L_RUN_ID
           LIMIT G_BATCHSIZE;
         L_LAST := L_CLIENT_NO.COUNT;
         EXIT WHEN L_LAST = 0;
         L_ROWNUMBER := L_ROWNUMBER + L_LAST;

         FORALL X IN 1 .. L_LAST
            INSERT INTO REPGA_DETM_OBA_QCONTC
               (SYM_RUN_DATE,
                CLIENT_NO,
                RUN_ID)
            VALUES
               (P_SYM_RUN_DATE,
                L_CLIENT_NO(X),
                L_RUN_ID(X));
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
      G_RUNID := REPGA_BACKOFFICE_PKG.START_API(P_APPNAME      => GC_APPNAME,
                                                P_SYM_RUN_DATE => P_SYM_RUN_DATE);
      G_PROCESSED :=0;
      PREPROCESS(P_SYM_RUN_DATE);
      DELETE_TABLE(P_SYM_RUN_DATE);
      INSERT_TABLE(P_SYM_RUN_DATE);
      POSTPROCESS(P_SYM_RUN_DATE);
      REPGA_BACKOFFICE_PKG.END_API(G_PROCESSED);
   EXCEPTION
      WHEN OTHERS THEN
         REPGA_BACKOFFICE_PKG.END_ERROR_API(SQLCODE,SQLERRM);
   END;

END;
/