CREATE OR REPLACE PACKAGE REPGA_STAM_OBA_QCONTC_PKG IS
  -- %version   0.1   2012.10.09
  -- %author   Vincze Csaba
  -- %usage loading package for stage area
  -- %intab tablelist:  REPGA_EXTM_OBA_QCONTC
  --
  -- %outtab tablelist: REPGA_STAM_OBA_QCONTC
  PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
CREATE OR REPLACE PACKAGE BODY REPGA_STAM_OBA_QCONTC_PKG IS
  -- %version   0.1   2012.10.09
  -- %author   Vincze Csaba

  -- Procedure for calculating and loading data into REPGA_STAM_OBA_QCONTC table
  -- %param p_sym_run_data: symbols running time <br>
  --                in form of date type
  -- %raises NO
  -- %dynasql YES
  -- %autonom NO
  -- %dbmsout NO
  GC_APPNAME CONSTANT VARCHAR2(30) := 'REPGA_STAM_OBA_QCONTC_PKG';

  GC_TARGETTABLE CONSTANT VARCHAR2(30) := 'REPGA_EXTM_OBA_QCONTC';

  GC_CSVNAME CONSTANT VARCHAR2(50) := 'REPGA_OBA_CLIENT.csv';

  GC_CSVDIR CONSTANT VARCHAR2(200) := 'DIR_FROMMANPRC_TOREPGA';

  GC_CSVARCHDIR CONSTANT VARCHAR2(200) := 'DIR_ARCH_FROMMANPRC_TOREPGA';

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

    TYPE T_SYM_RUN_DATE IS TABLE OF REPGA_STAM_LATE_PAY.SYM_RUN_DATE%TYPE INDEX BY PLS_INTEGER;
    TYPE T_CLIENT_NO IS TABLE OF REPGA_STAM_LATE_PAY.CLIENT_NO%TYPE INDEX BY PLS_INTEGER;

    L_SYM_RUN_DATE        T_SYM_RUN_DATE;
    L_CLIENT_NO           T_CLIENT_NO;

    CURSOR HC IS(
      SELECT P_SYM_RUN_DATE,
             REGEXP_REPLACE("client_no",
                            '([[:cntrl:]])|(^\t)',
                            NULL)
        FROM REPGA_EXTM_OBA_QCONTC
       WHERE "client_no" IS NOT NULL);

  BEGIN
    REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
    OPEN HC;
    LOOP
      REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION || ' ' || TO_CHAR(L_ROWNUMBER));
      L_SYM_RUN_DATE.DELETE;
      FETCH HC BULK COLLECT
        INTO L_SYM_RUN_DATE, L_CLIENT_NO LIMIT G_BATCHSIZE;
      L_LAST := L_SYM_RUN_DATE.COUNT;
      EXIT WHEN L_LAST = 0;
      L_ROWNUMBER := L_ROWNUMBER + L_LAST;

      FORALL X IN 1 .. L_LAST
        INSERT INTO REPGA_STAM_OBA_QCONTC
          (SYM_RUN_DATE,
           CLIENT_NO,
           RUN_ID)
        VALUES
          (L_SYM_RUN_DATE(X),
           L_CLIENT_NO(X),
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
    ARCHIVE_NAME := SUBSTR(GC_CSVNAME,
                           0,
                           LENGTH(GC_CSVNAME) - 4) || '_' ||
                    TO_CHAR(P_SYM_RUN_DATE,
                            'YYYYMMDD') || '.csv';

    UTL_FILE.FRENAME(GC_CSVDIR,
                     GC_CSVNAME,
                     GC_CSVARCHDIR,
                     ARCHIVE_NAME,
                     TRUE);
  END;

  ----------------------------------------
  PROCEDURE PREPROCESS(P_SYM_RUN_DATE DATE) IS
    C_ACCTION CONSTANT VARCHAR2(10) := 'PREPROCESS';
  BEGIN
    REPGA_BACKOFFICE_PKG.API_ACTION(C_ACCTION);
    G_BATCHSIZE   := REPGA_UTIL_PKG.GET_BATCHSIZE(GC_TARGETTABLE,
                                                  G_BATCHSIZE);
    G_COMMITPOINT := REPGA_UTIL_PKG.GET_COMMITPOINT(GC_TARGETTABLE,
                                                    G_COMMITPOINT);
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
    G_RUNID     := REPGA_BACKOFFICE_PKG.START_API(P_APPNAME      => GC_APPNAME,
                                                  P_SYM_RUN_DATE => P_SYM_RUN_DATE);
    G_PROCESSED := 0;
    PREPROCESS(P_SYM_RUN_DATE);

    IF REPGA_UTIL_PKG.IS_FILE_AVAILABLE(GC_CSVDIR,
                                        GC_CSVNAME) THEN
      DELETE_TABLE(P_SYM_RUN_DATE);
      INSERT_TABLE(P_SYM_RUN_DATE);
      RENAME_DATAFILE(P_SYM_RUN_DATE);
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
