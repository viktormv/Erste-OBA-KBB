CREATE OR REPLACE PACKAGE REPGA_DETM_OBA_RES_URB_B_PKG IS
  -- %version   0.1   2012.10.11
  -- %author   Vincze Csaba
  -- %usage loading package for stage area
  -- %intab tablelist:  REPGA_DETM_OBA_TBASE,
  --                    REPGA_DETM_OBA_U_BETET
  --
  -- %outtab tablelist: REPGA_DETM_OBA_RES_URB_B
  PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_DETM_OBA_RES_URB_B_PKG IS
  -- %version   0.1   2012.10.11
  -- %author   Vincze Csaba

  -- Procedure for calculating and loading data into REPGA_DETM_OBA_RES_URB_B table
  -- %param p_sym_run_data: symbols running time <br>
  --                in form of date type
  -- %raises NO
  -- %dynasql YES
  -- %autonom NO
  -- %dbmsout NO
  ------------------------------------------------------------------------
  --   0.1 Vincze Csaba        Intitial

   GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_RES_URB_B_PKG';

   GC_TARGETTABLE CONSTANT VARCHAR2(30):='REPGA_DETM_OBA_RES_URB_B';

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

    TYPE T_SYM_RUN_DATE  IS TABLE OF REPGA_DETM_OBA_RES_URB_B.SYM_RUN_DATE%TYPE INDEX BY PLS_INTEGER;
    TYPE T_CLIENT_NO     IS TABLE OF REPGA_DETM_OBA_RES_URB_B.CLIENT_NO%TYPE INDEX BY PLS_INTEGER;
    TYPE T_SZLA          IS TABLE OF REPGA_DETM_OBA_RES_URB_B.SZLA%TYPE INDEX BY PLS_INTEGER;
    TYPE T_UA            IS TABLE OF REPGA_DETM_OBA_RES_URB_B.UA%TYPE INDEX BY PLS_INTEGER;
    TYPE T_TIPUS         IS TABLE OF REPGA_DETM_OBA_RES_URB_B.TIPUS%TYPE INDEX BY PLS_INTEGER;
    TYPE T_BJOGCIM       IS TABLE OF REPGA_DETM_OBA_RES_URB_B.BJOGCIM%TYPE INDEX BY PLS_INTEGER;
    TYPE T_EGYENLEG      IS TABLE OF REPGA_DETM_OBA_RES_URB_B.EGYENLEG%TYPE INDEX BY PLS_INTEGER;
    TYPE T_KAMAT         IS TABLE OF REPGA_DETM_OBA_RES_URB_B.KAMAT%TYPE INDEX BY PLS_INTEGER;
    TYPE T_FORRASADO     IS TABLE OF REPGA_DETM_OBA_RES_URB_B.FORRASADO%TYPE INDEX BY PLS_INTEGER;
    TYPE T_ZAROLT        IS TABLE OF REPGA_DETM_OBA_RES_URB_B.ZAROLT%TYPE INDEX BY PLS_INTEGER;

    L_SYM_RUN_DATE      T_SYM_RUN_DATE;
    L_CLIENT_NO         T_CLIENT_NO;
    L_SZLA              T_SZLA;
    L_UA                T_UA;
    L_TIPUS             T_TIPUS;
    L_BJOGCIM           T_BJOGCIM;
    L_EGYENLEG          T_EGYENLEG;
    L_KAMAT             T_KAMAT;
    L_FORRASADO         T_FORRASADO;
    L_ZAROLT            T_ZAROLT;

      CURSOR HC IS(
                    SELECT B.SYM_RUN_DATE, client_no, szla, NULL ua, s.tipus,
                           s.bjogcim,
                           s.egyenleg,
                           s.kamat,
                           s.forrasado,
                           s.zarolt
                      FROM REPGA_DETM_OBA_TBASE b, REPGA_DETM_OBA_U_BETET s
                     WHERE B.SYM_RUN_DATE = S.SYM_RUN_DATE
                       AND b.src_sys = 2
                       AND s.client_no = b.azonosito
                       AND B.SYM_RUN_DATE = P_SYM_RUN_DATE
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
                  L_CLIENT_NO,
                  L_SZLA,
                  L_UA,
                  L_TIPUS,
                  L_BJOGCIM,
                  L_EGYENLEG,
                  L_KAMAT,
                  L_FORRASADO,
                  L_ZAROLT
            LIMIT G_BATCHSIZE;
         L_LAST := L_SYM_RUN_DATE.COUNT;
         EXIT WHEN L_LAST = 0;
         L_ROWNUMBER := L_ROWNUMBER + L_LAST;

         FORALL X IN 1 .. L_LAST
            INSERT INTO REPGA_DETM_OBA_RES_URB_B
                 (SYM_RUN_DATE,
                  CLIENT_NO,
                  SZLA,
                  UA,
                  TIPUS,
                  BJOGCIM,
                  EGYENLEG,
                  KAMAT,
                  FORRASADO,
                  ZAROLT,
                  RUN_ID)
            VALUES
              (L_SYM_RUN_DATE(X),
               L_CLIENT_NO(X),
               L_SZLA(X),
               L_UA(X),
               L_TIPUS(X),
               L_BJOGCIM(X),
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
