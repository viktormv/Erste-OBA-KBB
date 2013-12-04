CREATE OR REPLACE PACKAGE REPGA_DETM_OBA_RES_B_PKG IS
  -- %version   0.1   2012.10.10
  -- %author   Vincze Csaba
  -- %usage loading package for stage area
  -- %intab tablelist:  REPGA_DETM_OBA_TBASE,
  --                    REPGA_DETM_OBA_FIDB_R,
  --                    REPGA_DETM_OBA_U_BETET,
  --                    REPGA_DETM_OBA_BJEGY
  --                    REPGA_DETM_OBA_EJEGY
  --
  -- %outtab tablelist: REPGA_DETM_OBA_RES_B
  PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_DETM_OBA_RES_B_PKG IS
  -- %version   0.1   2012.10.10
  -- %author   Vincze Csaba

  -- Procedure for calculating and loading data into REPGA_DETM_OBA_RES_B table
  -- %param p_sym_run_data: symbols running time <br>
  --                in form of date type
  -- %raises NO
  -- %dynasql YES
  -- %autonom NO
  -- %dbmsout NO
  ------------------------------------------------------------------------
  --   0.1 Vincze Csaba        Intitial

   GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_DETM_OBA_RES_B_PKG';

   GC_TARGETTABLE CONSTANT VARCHAR2(30):='REPGA_DETM_OBA_RES_B';

   GC_EUR_LIMIT NUMBER := 100000; -- OBA limit 100 000.- EUR

   G_HUF_LIMIT NUMBER := 0;

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

    TYPE T_SYM_RUN_DATE IS TABLE OF REPGA_DETM_OBA_RES_B.SYM_RUN_DATE%TYPE INDEX BY PLS_INTEGER;
    TYPE T_NEW_CSOP_ID  IS TABLE OF REPGA_DETM_OBA_RES_B.NEW_CSOP_ID%TYPE INDEX BY PLS_INTEGER;
    TYPE T_OBA_ORDER    IS TABLE OF REPGA_DETM_OBA_RES_B.OBA_ORDER%TYPE INDEX BY PLS_INTEGER;
    TYPE T_ACCOUNT_NO   IS TABLE OF REPGA_DETM_OBA_RES_B.ACCOUNT_NO%TYPE INDEX BY PLS_INTEGER;
    TYPE T_UA           IS TABLE OF REPGA_DETM_OBA_RES_B.UA%TYPE INDEX BY PLS_INTEGER;
    TYPE T_TIPUS        IS TABLE OF REPGA_DETM_OBA_RES_B.TIPUS%TYPE INDEX BY PLS_INTEGER;
    TYPE T_BJOGCIM      IS TABLE OF REPGA_DETM_OBA_RES_B.BJOGCIM%TYPE INDEX BY PLS_INTEGER;
    TYPE T_BAL_HUF      IS TABLE OF REPGA_DETM_OBA_RES_B.BAL_HUF%TYPE INDEX BY PLS_INTEGER;
    TYPE T_INT_HUF      IS TABLE OF REPGA_DETM_OBA_RES_B.INT_HUF%TYPE INDEX BY PLS_INTEGER;
    TYPE T_INT_TAX      IS TABLE OF REPGA_DETM_OBA_RES_B.INT_TAX%TYPE INDEX BY PLS_INTEGER;
    TYPE T_RESTRAINT    IS TABLE OF REPGA_DETM_OBA_RES_B.RESTRAINT%TYPE INDEX BY PLS_INTEGER;


    L_SYM_RUN_DATE  T_SYM_RUN_DATE;
    L_NEW_CSOP_ID   T_NEW_CSOP_ID;
    L_OBA_ORDER     T_OBA_ORDER;
    L_ACCOUNT_NO    T_ACCOUNT_NO;
    L_UA            T_UA;
    L_TIPUS         T_TIPUS;
    L_BJOGCIM       T_BJOGCIM;
    L_BAL_HUF       T_BAL_HUF;
    L_INT_HUF       T_INT_HUF;
    L_INT_TAX       T_INT_TAX;
    L_RESTRAINT     T_RESTRAINT;

      CURSOR HC IS(
                    /*SELECT NULL "KBB", NULL "Kártalanítási sorrend száma", NULL "Egyedi betétszámlaszám", NULL "Ügylet azonosító", NULL "Termék típus", NULL "Biztosítási jogcím", NULL "Számla tokeegyenleg", NULL "Számla kamata", NULL "Kamatjövedelemadó", NULL "Zárolt összeg"
                      FROM dual
                     WHERE ROWNUM < 1
                    UNION ALL*/
                    SELECT B.SYM_RUN_DATE,
                           b.new_csop_id,
                           (
                           SELECT r_no
                             FROM REPGA_DETM_OBA_TBASE_ORD
                            WHERE new_csop_id = b.new_csop_id
                              AND azonosito = b.azonosito
                              AND account_no = s.account_no
                              AND SYM_RUN_DATE = P_SYM_RUN_DATE
                           ) oba_order,
                           account_no, NULL ua, s.tipus,
                           DECODE(ING,'Y','ING','ERSTE') bjogcim,
                           bal_huf,
                           int_huf,
                           int_tax intfee,
                           restraint rest
                      FROM REPGA_DETM_OBA_TBASE b, REPGA_DETM_OBA_FIDB_R s
                     WHERE B.SYM_RUN_DATE = S.SYM_RUN_DATE
                       AND b.src_sys = 0
                       AND s.client_no = b.azonosito
                       AND B.SYM_RUN_DATE = P_SYM_RUN_DATE
                    UNION ALL
                    SELECT B.SYM_RUN_DATE,
                           b.new_csop_id,
                           (
                           SELECT r_no
                             FROM REPGA_DETM_OBA_TBASE_ORD
                            WHERE new_csop_id = b.new_csop_id
                              AND azonosito = b.azonosito
                              AND account_no = SUBSTR(s.szla,9,12)
                              AND tipus = s.tipus
                              AND SYM_RUN_DATE = P_SYM_RUN_DATE
                           ) oba_order,
                           szla, NULL ua, s.tipus,
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
                    UNION ALL
                    SELECT B.SYM_RUN_DATE,
                           b.new_csop_id,
                           (
                           SELECT r_no
                             FROM REPGA_DETM_OBA_TBASE_ORD
                            WHERE new_csop_id = b.new_csop_id
                              AND azonosito = b.azonosito
                              AND account_no = s.f2
                              AND SYM_RUN_DATE = P_SYM_RUN_DATE
                           ) oba_order,
                           f2 account_no, NULL ua, 'B' tipus,
                           'ERSTE' bjogcim,
                           s.egyenleg,
                           s.kamat,
                           s.forrasado,
                           s.zarolt
                      FROM REPGA_DETM_OBA_TBASE b, REPGA_DETM_OBA_BJEGY s
                     WHERE B.SYM_RUN_DATE = S.SYM_RUN_DATE
                       AND b.src_sys = 1
                       AND s.f1 = b.azonosito
                       AND B.SYM_RUN_DATE = P_SYM_RUN_DATE
                    UNION ALL
                    SELECT B.SYM_RUN_DATE,
                           b.new_csop_id,
                           (
                           SELECT r_no
                             FROM REPGA_DETM_OBA_TBASE_ORD
                            WHERE new_csop_id = b.new_csop_id
                              AND azonosito = b.azonosito
                              AND account_no = s.f2
                              AND SYM_RUN_DATE = P_SYM_RUN_DATE
                           ) oba_order,
                           f2 account_no, NULL ua, 'B' tipus,
                           'ERSTE' bjogcim,
                           s.egyenleg,
                           s.kamat,
                           s.forrasado,
                           s.zarolt
                      FROM REPGA_DETM_OBA_TBASE b, REPGA_DETM_OBA_EJEGY s
                     WHERE B.SYM_RUN_DATE = S.SYM_RUN_DATE
                       AND b.src_sys = 3
                       AND s.f1 = b.azonosito
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
                  L_NEW_CSOP_ID,
                  L_OBA_ORDER,
                  L_ACCOUNT_NO,
                  L_UA,
                  L_TIPUS,
                  L_BJOGCIM,
                  L_BAL_HUF,
                  L_INT_HUF,
                  L_INT_TAX,
                  L_RESTRAINT
            LIMIT G_BATCHSIZE;
         L_LAST := L_SYM_RUN_DATE.COUNT;
         EXIT WHEN L_LAST = 0;
         L_ROWNUMBER := L_ROWNUMBER + L_LAST;

         FORALL X IN 1 .. L_LAST
            INSERT INTO REPGA_DETM_OBA_RES_B
                 (SYM_RUN_DATE,
                  NEW_CSOP_ID,
                  OBA_ORDER,
                  ACCOUNT_NO,
                  UA,
                  TIPUS,
                  BJOGCIM,
                  BAL_HUF,
                  INT_HUF,
                  INT_TAX,
                  RESTRAINT,
                  RUN_ID)
            VALUES
              (L_SYM_RUN_DATE(X),
               L_NEW_CSOP_ID(X),
               L_OBA_ORDER(X),
               L_ACCOUNT_NO(X),
               L_UA(X),
               L_TIPUS(X),
               L_BJOGCIM(X),
               L_BAL_HUF(X),
               L_INT_HUF(X),
               L_INT_TAX(X),
               L_RESTRAINT(X),
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

     G_HUF_LIMIT := CONV_AMT_MTH(P_SYM_RUN_DATE, 'EUR', GC_EUR_LIMIT, 'HUF');

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