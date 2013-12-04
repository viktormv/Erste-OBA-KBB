CREATE OR REPLACE PACKAGE REPGA_DETM_OBA_UPLOAD_PKG IS
	-- %version   0.1   2012.10.25.
	-- %author   Dörnyei László
	-- %usage loading package for detail area
	-- %intab tablelist:  
	--                    REPGA_DETM_OBA_UGYFEL
	--                    REPGA_DETM_OBA_UGYLET
	-- %outtab tablelist: REPGA_DETM_OBA_UGYLET
	--                    REPGA_DETM_OBA_UGYFEL

	PROCEDURE MAIN(P_SYM_RUN_DATE DATE DEFAULT EBH_PWM_PUBLIC_P.F_GETSYMRUNDATE);

END;
/
