CREATE OR REPLACE PACKAGE REPGA_STAM_OBA_U_CLIENT_PKG IS
  -- %version   0.1   2012.10.10
  -- %author   Vincze Csaba
  -- %usage loading package for stage area
  -- %intab tablelist:  REPGA_EXTM_OBA_U_CLIENT 
  --
  -- %outtab tablelist: REPGA_STAM_OBA_U_CLIENT
  PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_STAM_OBA_U_CLIENT_PKG IS
  -- %version   0.1   2012.10.10
  -- %author   Vincze Csaba

  -- Procedure for calculating and loading data into REPGA_STAM_OBA_U_CLIENT table
  -- %param p_sym_run_data: symbols running time <br>
  --                in form of date type
  -- %raises NO
  -- %dynasql YES
  -- %autonom NO
  -- %dbmsout NO
  GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_STAM_OBA_U_CLIENT_PKG';

  GC_SOURCETABLE CONSTANT VARCHAR2(30) := 'REPGA_EXTM_OBA_U_CLIENT';

  GC_TARGETTABLE CONSTANT VARCHAR2(30) := 'REPGA_STAM_OBA_U_CLIENT';

  GC_CSVDIR CONSTANT VARCHAR2(200) := 'DIR_FROMURBIS_TOOBA.SQL';

  GC_CSVARCHDIR CONSTANT VARCHAR2(200) := 'DIR_ARCH_FROMMANPRC_TOREPGA';

  G_CSVNAME VARCHAR2(50) := 'URB_OBA_CLIENT_';

  G_PROCESSED NUMBER := 0;

  G_RUNID NUMBER;

  G_COMMITPOINT NUMBER := 20000;

  G_BATCHSIZE NUMBER := 5000;

  ----------------------------------------
  -- %version   0.1   VCS Initial

  PROCEDURE DELETE_TABLE(P_SYM_RUN_DATE DATE) IS
    C_ACCTION CONSTANT VARCHAR2(6) := 'DELETE';
  BEGIN
    REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
    REPGA_UTIL_PKG.DELETE_TABLE(GC_TARGETTABLE,
                                P_SYM_RUN_DATE);
  END;

  ----------------------------------------
  PROCEDURE INSERT_TABLE(P_SYM_RUN_DATE DATE) IS
    C_ACCTION CONSTANT VARCHAR2(8) := 'INSERT';
    L_ROWNUMBER NUMBER := 0;
    L_INDEX     NUMBER := 0;
    L_LAST      NUMBER;

    TYPE T_SYM_RUN_DATE  IS TABLE OF REPGA_STAM_OBA_U_CLIENT.SYM_RUN_DATE%TYPE INDEX BY PLS_INTEGER;
    TYPE T_RNO           IS TABLE OF REPGA_STAM_OBA_U_CLIENT.RNO%TYPE INDEX BY PLS_INTEGER;
    TYPE T_CLIENT_NO     IS TABLE OF REPGA_STAM_OBA_U_CLIENT.CLIENT_NO%TYPE INDEX BY PLS_INTEGER;
    TYPE T_SZL           IS TABLE OF REPGA_STAM_OBA_U_CLIENT.SZL%TYPE INDEX BY PLS_INTEGER;
    TYPE T_TUL_H         IS TABLE OF REPGA_STAM_OBA_U_CLIENT.TUL_H%TYPE INDEX BY PLS_INTEGER;
    TYPE T_TIPUS         IS TABLE OF REPGA_STAM_OBA_U_CLIENT.TIPUS%TYPE INDEX BY PLS_INTEGER;
    TYPE T_NEV           IS TABLE OF REPGA_STAM_OBA_U_CLIENT.NEV%TYPE INDEX BY PLS_INTEGER;
    TYPE T_ELHAL_DT      IS TABLE OF REPGA_STAM_OBA_U_CLIENT.ELHAL_DT%TYPE INDEX BY PLS_INTEGER;
    TYPE T_SZULNEV       IS TABLE OF REPGA_STAM_OBA_U_CLIENT.SZULNEV%TYPE INDEX BY PLS_INTEGER;
    TYPE T_ANYANEV       IS TABLE OF REPGA_STAM_OBA_U_CLIENT.ANYANEV%TYPE INDEX BY PLS_INTEGER;
    TYPE T_SZUL_IDO      IS TABLE OF REPGA_STAM_OBA_U_CLIENT.SZUL_IDO%TYPE INDEX BY PLS_INTEGER;
    TYPE T_SZUL_HELY     IS TABLE OF REPGA_STAM_OBA_U_CLIENT.SZUL_HELY%TYPE INDEX BY PLS_INTEGER;
    TYPE T_CJK           IS TABLE OF REPGA_STAM_OBA_U_CLIENT.CJK%TYPE INDEX BY PLS_INTEGER;
    TYPE T_REZIDENS      IS TABLE OF REPGA_STAM_OBA_U_CLIENT.REZIDENS%TYPE INDEX BY PLS_INTEGER;
    TYPE T_SZIGSZ        IS TABLE OF REPGA_STAM_OBA_U_CLIENT.SZIGSZ%TYPE INDEX BY PLS_INTEGER;
    TYPE T_UTLEVSZ       IS TABLE OF REPGA_STAM_OBA_U_CLIENT.UTLEVSZ%TYPE INDEX BY PLS_INTEGER;
    TYPE T_VEZ_ENG       IS TABLE OF REPGA_STAM_OBA_U_CLIENT.VEZ_ENG%TYPE INDEX BY PLS_INTEGER;
    TYPE T_NYILV         IS TABLE OF REPGA_STAM_OBA_U_CLIENT.NYILV%TYPE INDEX BY PLS_INTEGER;
    TYPE T_KSH           IS TABLE OF REPGA_STAM_OBA_U_CLIENT.KSH%TYPE INDEX BY PLS_INTEGER;
    TYPE T_ADOSZAM       IS TABLE OF REPGA_STAM_OBA_U_CLIENT.ADOSZAM%TYPE INDEX BY PLS_INTEGER;
    TYPE T_IRSZ          IS TABLE OF REPGA_STAM_OBA_U_CLIENT.IRSZ%TYPE INDEX BY PLS_INTEGER;
    TYPE T_CIMSOR1_2     IS TABLE OF REPGA_STAM_OBA_U_CLIENT.CIMSOR1_2%TYPE INDEX BY PLS_INTEGER;
    TYPE T_CIMSOR3_5     IS TABLE OF REPGA_STAM_OBA_U_CLIENT.CIMSOR3_5%TYPE INDEX BY PLS_INTEGER;
    TYPE T_EIRSZ         IS TABLE OF REPGA_STAM_OBA_U_CLIENT.EIRSZ%TYPE INDEX BY PLS_INTEGER;
    TYPE T_ECIMSOR1_2    IS TABLE OF REPGA_STAM_OBA_U_CLIENT.ECIMSOR1_2%TYPE INDEX BY PLS_INTEGER;
    TYPE T_ECIMSOR3_5    IS TABLE OF REPGA_STAM_OBA_U_CLIENT.ECIMSOR3_5%TYPE INDEX BY PLS_INTEGER;



    L_SYM_RUN_DATE       T_SYM_RUN_DATE;
    L_RNO                T_RNO;
    L_CLIENT_NO          T_CLIENT_NO;
    L_SZL                T_SZL;
    L_TUL_H              T_TUL_H;
    L_TIPUS              T_TIPUS;
    L_NEV                T_NEV;
    L_ELHAL_DT           T_ELHAL_DT;
    L_SZULNEV            T_SZULNEV;
    L_ANYANEV            T_ANYANEV;
    L_SZUL_IDO           T_SZUL_IDO;
    L_SZUL_HELY          T_SZUL_HELY;
    L_CJK                T_CJK;
    L_REZIDENS           T_REZIDENS;
    L_SZIGSZ             T_SZIGSZ;
    L_UTLEVSZ            T_UTLEVSZ;
    L_VEZ_ENG            T_VEZ_ENG;
    L_NYILV              T_NYILV;
    L_KSH                T_KSH;
    L_ADOSZAM            T_ADOSZAM;
    L_IRSZ               T_IRSZ;
    L_CIMSOR1_2          T_CIMSOR1_2;
    L_CIMSOR3_5          T_CIMSOR3_5;
    L_EIRSZ              T_EIRSZ;
    L_ECIMSOR1_2         T_ECIMSOR1_2;
    L_ECIMSOR3_5         T_ECIMSOR3_5;

    CURSOR HC IS(
      SELECT P_SYM_RUN_DATE,
             RNO,
             CLIENT_NO,
             SZL,
             TUL_H,
             TIPUS,
             NEV,
             ELHAL_DT,
             SZULNEV,
             ANYANEV,
             SZUL_IDO,
             SZUL_HELY,
             CJK,
             REZIDENS,
             SZIGSZ,
             UTLEVSZ,
             VEZ_ENG,
             NYILV,
             KSH,
             ADOSZAM,
             IRSZ,
             CIMSOR1_2,
             CIMSOR3_5,
             EIRSZ,
             ECIMSOR1_2,
             REGEXP_REPLACE(ECIMSOR3_5,
                            '([[:cntrl:]])|(^\t)',
                            NULL)
        FROM REPGA_EXTM_OBA_U_CLIENT
                );

  BEGIN
    REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
    OPEN HC;
    LOOP
      REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' || TO_CHAR(L_ROWNUMBER));
      L_SYM_RUN_DATE.DELETE;
      FETCH HC BULK COLLECT
        INTO L_SYM_RUN_DATE,
             L_RNO,
             L_CLIENT_NO,
             L_SZL,
             L_TUL_H,
             L_TIPUS,
             L_NEV,
             L_ELHAL_DT,
             L_SZULNEV,
             L_ANYANEV,
             L_SZUL_IDO,
             L_SZUL_HELY,
             L_CJK,
             L_REZIDENS,
             L_SZIGSZ,
             L_UTLEVSZ,
             L_VEZ_ENG,
             L_NYILV,
             L_KSH,
             L_ADOSZAM,
             L_IRSZ,
             L_CIMSOR1_2,
             L_CIMSOR3_5,
             L_EIRSZ,
             L_ECIMSOR1_2,
             L_ECIMSOR3_5 LIMIT G_BATCHSIZE;
      L_LAST := L_SYM_RUN_DATE.COUNT;
      EXIT WHEN L_LAST = 0;
      L_ROWNUMBER := L_ROWNUMBER + L_LAST;

      FORALL X IN 1 .. L_LAST
        INSERT INTO REPGA_STAM_OBA_U_CLIENT
          (SYM_RUN_DATE,
           RNO,
           CLIENT_NO,
           SZL,
           TUL_H,
           TIPUS,
           NEV,
           ELHAL_DT,
           SZULNEV,
           ANYANEV,
           SZUL_IDO,
           SZUL_HELY,
           CJK,
           REZIDENS,
           SZIGSZ,
           UTLEVSZ,
           VEZ_ENG,
           NYILV,
           KSH,
           ADOSZAM,
           IRSZ,
           CIMSOR1_2,
           CIMSOR3_5,
           EIRSZ,
           ECIMSOR1_2,
           ECIMSOR3_5,
           RUN_ID)
        VALUES
          ( L_SYM_RUN_DATE(X),
            L_RNO(X),
            L_CLIENT_NO(X),
            L_SZL(X),
            L_TUL_H(X),
            L_TIPUS(X),
            L_NEV(X),
            L_ELHAL_DT(X),
            L_SZULNEV(X),
            L_ANYANEV(X),
            L_SZUL_IDO(X),
            L_SZUL_HELY(X),
            L_CJK(X),
            L_REZIDENS(X),
            L_SZIGSZ(X),
            L_UTLEVSZ(X),
            L_VEZ_ENG(X),
            L_NYILV(X),
            L_KSH(X),
            L_ADOSZAM(X),
            L_IRSZ(X),
            L_CIMSOR1_2(X),
            L_CIMSOR3_5(X),
            L_EIRSZ(X),
            L_ECIMSOR1_2(X),
            L_ECIMSOR3_5(X),
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
    G_PROCESSED := G_PROCESSED + L_ROWNUMBER;
  END;

  ----------------------------------------
  PROCEDURE RENAME_DATAFILE(P_SYM_RUN_DATE DATE) IS
    C_ACCTION CONSTANT VARCHAR2(15) := 'RENAME_DATAFILE';
    ARCHIVE_NAME VARCHAR2(50);
  BEGIN
    REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
    ARCHIVE_NAME := SUBSTR(G_CSVNAME,
                           0,
                           LENGTH(G_CSVNAME) - 4) || '_' ||
                    TO_CHAR(P_SYM_RUN_DATE,
                            'YYYYMMDD') || '.csv';

    UTL_FILE.FRENAME(GC_CSVDIR,
                     G_CSVNAME,
                     GC_CSVARCHDIR,
                     ARCHIVE_NAME,
                     TRUE);
  END;

  ----------------------------------------
  PROCEDURE PREPROCESS(P_SYM_RUN_DATE DATE) IS
    C_ACCTION CONSTANT VARCHAR2(10) := 'PREPROCESS';
    L_SYM_RUN_DATE VARCHAR2(8);
  BEGIN
    REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
    G_BATCHSIZE   := REPGA_UTIL_PKG.GET_BATCHSIZE(GC_TARGETTABLE,
                                                  G_BATCHSIZE);
    G_COMMITPOINT := REPGA_UTIL_PKG.GET_COMMITPOINT(GC_TARGETTABLE,
                                                    G_COMMITPOINT);
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
    G_RUNID     := REPGA_BACKOFFICE_PKG.START_API(P_APPNAME      => GC_APPNAME,
                                                  P_SYM_RUN_DATE => P_SYM_RUN_DATE);
    G_PROCESSED := 0;
    PREPROCESS(P_SYM_RUN_DATE);

    IF REPGA_UTIL_PKG.IS_FILE_AVAILABLE(GC_CSVDIR,
                                        G_CSVNAME) THEN
      DELETE_TABLE(P_SYM_RUN_DATE);
      INSERT_TABLE(P_SYM_RUN_DATE);
--      RENAME_DATAFILE(P_SYM_RUN_DATE);
    ELSE
      REPGA_BACKOFFICE_PKG.API_MSG(0,
                                   'CSV read error...');
    END IF;

    POSTPROCESS(P_SYM_RUN_DATE);
    REPGA_BACKOFFICE_PKG.END_API(G_PROCESSED);
  EXCEPTION
    WHEN OTHERS THEN
      REPGA_BACKOFFICE_PKG.END_ERROR_API(SQLCODE,
                                         SQLERRM);
  END;

END;
/
