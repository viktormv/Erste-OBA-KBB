CREATE OR REPLACE PACKAGE REPGA_STAM_OBA_U_BETET_PKG IS
  -- %version   0.3   2012.12.17
  -- %author   Vincze Csaba
  -- %usage loading package for stage area
  -- %intab tablelist:  REPGA_STAM_OBA_U_BETET
  --
  -- %outtab tablelist: REPGA_DETM_OBA_U_BETET
  PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_STAM_OBA_U_BETET_PKG IS
  -- %version   0.1   2012.10.10
  -- %version   0.2   2012.12.04
  -- %version   0.3   2012.12.17
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
  --   0.2 Vincze Csaba        CSV javitas
  --   0.3 Vincze Csaba        GC_TARGETTABLE + GC_APPNAME + GC_CSVDIR javitas

   GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_STAM_OBA_U_BETET_PKG';

   GC_TARGETTABLE CONSTANT VARCHAR2(30):='REPGA_STAM_OBA_U_BETET';

   GC_CSVDIR CONSTANT VARCHAR2(200) := 'DIR_FROMURBIS_TOOBA';

   GC_SOURCETABLE CONSTANT VARCHAR2(30) := 'REPGA_EXTM_OBA_U_BETET';

   G_CSVNAME VARCHAR2(50) := 'URB_OBA_BETET_';

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
--      TYPE T_RNO             IS TABLE OF REPGA_DETM_OBA_U_BETET.RNO%TYPE INDEX BY PLS_INTEGER;
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
--    L_RNO             T_RNO;
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
                    SELECT P_SYM_RUN_DATE,
              --             RNO,
                           CLIENT_NO,
                           SZLA,
                           UGYLET,
                           TIPUS,
                           BJOGCIM,
                           EGYENLEG,
                           KAMAT,
                           FORRASADO,
                           REGEXP_REPLACE(ZAROLT,
                                          '([[:cntrl:]])|(^\t)',
                                          NULL)
                      FROM REPGA_EXTM_OBA_U_BETET
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
--             L_RNO,
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
            INSERT INTO REPGA_STAM_OBA_U_BETET
                (SYM_RUN_DATE,
--                 RNO,
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
--                L_RNO(X),
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
   PROCEDURE PREPROCESS(P_SYM_RUN_DATE DATE) IS
      C_ACCTION CONSTANT VARCHAR2(10) := 'PREPROCESS';
      L_SYM_RUN_DATE VARCHAR2(8);
   BEGIN
      REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
      G_BATCHSIZE   := REPGA_UTIL_PKG.GET_BATCHSIZE(GC_TARGETTABLE,  G_BATCHSIZE);
      G_COMMITPOINT := REPGA_UTIL_PKG.GET_COMMITPOINT(GC_TARGETTABLE,G_COMMITPOINT);
      REPGA_UTIL_PKG.CLEANUP(P_SYM_RUN_DATE => P_SYM_RUN_DATE,
                             P_APPNAME      => GC_APPNAME,
                             P_TARGETNAME   => GC_TARGETTABLE);

      L_SYM_RUN_DATE := TO_CHAR(P_SYM_RUN_DATE,'YYYYMMDD');
      G_CSVNAME := G_CSVNAME||L_SYM_RUN_DATE||'.CSV';

      EXECUTE IMMEDIATE 'ALTER TABLE '||GC_SOURCETABLE||' LOCATION ('''||G_CSVNAME||''')';
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

      IF REPGA_UTIL_PKG.IS_FILE_AVAILABLE(GC_CSVDIR,
                                          G_CSVNAME) THEN
        DELETE_TABLE(P_SYM_RUN_DATE);
        INSERT_TABLE(P_SYM_RUN_DATE);
--        RENAME_DATAFILE(P_SYM_RUN_DATE);
      ELSE
        REPGA_BACKOFFICE_PKG.API_MSG(0,
                                     'CSV read error...');
      END IF;

      POSTPROCESS(P_SYM_RUN_DATE);
      REPGA_BACKOFFICE_PKG.END_API(G_PROCESSED);
   EXCEPTION
      WHEN OTHERS THEN
         REPGA_BACKOFFICE_PKG.END_ERROR_API(SQLCODE,SQLERRM);
   END;

END;
/
