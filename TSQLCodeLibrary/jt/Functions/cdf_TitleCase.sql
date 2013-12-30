CREATE FUNCTION [jt].[cdf_TitleCase]
( @pStr AS NVARCHAR(100)
) RETURNS NVARCHAR(100) AS
BEGIN
/*
<objectname>cdf_TitleCase</objectname> 
<summary> 
Returns Title Case Formatted String
</summary> 
<parameters>
	<param name="@pStr">Input String</param>
</parameters><history>  
<entry version="1.0.0.2" date="2007-03-11" name="DavidP" action="COALESCE '' instead OF null"/> 
<entry version="1.0.0.1" date="2007-03-03" name="DavidP" action="Added punctuation characters"/> 
<entry version="1.0.0.0" date="2006-01-01" name="Conchango" action="Created"/> 
</history> 

SELECT dbo.cdf_TitleCase('mr.bean')

*/

DECLARE	@vReturnValue	AS NVARCHAR(100),
		@vPos AS TINYINT,
		@vPos1 AS TINYINT,
		@vLen AS TINYINT

SELECT	@vReturnValue = ' ' + LOWER(@pStr),
		@vPos = 1,
		@vLen = LEN(@pStr) + 1

WHILE @vPos > 0 AND @vPos <= @vLen
BEGIN
	SET @vReturnValue = STUFF(@vReturnValue,
						@vPos + 1,
						1,
						UPPER(SUBSTRING(@vReturnValue,@vPos + 1, 1)))

	SET @vPos1 = PATINDEX('%[ _/().-]%',SUBSTRING(@vReturnValue, @vPos + 1,100))
	SET @vPos = @vPos1 + SIGN(@vPos1) * @vPos

 END

RETURN COALESCE(RIGHT(@vReturnValue, @vLen - 1),'')
		
END