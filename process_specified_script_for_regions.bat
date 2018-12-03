title Process a specified script (parameter 1) for a set of study regions (following parameters)
echo  Process a specified script (parameter 1) for a set of study regions (following parameters)

echo all: %*
for /f "tokens=1,* delims= " %%a in ("%*") do set ALL_BUT_FIRST=%%b
echo Processing %1 for study regions: %ALL_BUT_FIRST%

FOR %%A  IN (%ALL_BUT_FIRST%) DO (
  python %1 %%A
)

@pause




