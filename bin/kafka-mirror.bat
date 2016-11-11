@if "%DEBUG%" == "" @echo off
@rem ##########################################################################
@rem
@rem  kafka-mirror startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

@rem Add default JVM options here. You can also use JAVA_OPTS and KAFKA_MIRROR_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS=

set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%..

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if "%ERRORLEVEL%" == "0" goto init

echo.
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto init

echo.
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:init
@rem Get command-line arguments, handling Windows variants

if not "%OS%" == "Windows_NT" goto win9xME_args
if "%@eval[2+2]" == "4" goto 4NT_args

:win9xME_args
@rem Slurp the command line arguments.
set CMD_LINE_ARGS=
set _SKIP=2

:win9xME_args_slurp
if "x%~1" == "x" goto execute

set CMD_LINE_ARGS=%*
goto execute

:4NT_args
@rem Get arguments from the 4NT Shell from JP Software
set CMD_LINE_ARGS=%$

:execute
@rem Setup the command line

set CLASSPATH=%APP_HOME%\lib\kafka-mirror-2.0.1.jar;%APP_HOME%\lib\storm-core-1.0.1.jar;%APP_HOME%\lib\commons-cli-1.3.1.jar;%APP_HOME%\lib\kafka_2.11-0.9.0.1.jar;%APP_HOME%\lib\storm-kafka-1.0.1.jar;%APP_HOME%\lib\kryo-3.0.3.jar;%APP_HOME%\lib\clojure-1.7.0.jar;%APP_HOME%\lib\disruptor-3.3.2.jar;%APP_HOME%\lib\log4j-api-2.1.jar;%APP_HOME%\lib\log4j-core-2.1.jar;%APP_HOME%\lib\log4j-slf4j-impl-2.1.jar;%APP_HOME%\lib\log4j-over-slf4j-1.6.6.jar;%APP_HOME%\lib\servlet-api-2.5.jar;%APP_HOME%\lib\slf4j-api-1.7.7.jar;%APP_HOME%\lib\zkclient-0.7.jar;%APP_HOME%\lib\kafka-clients-0.9.0.1.jar;%APP_HOME%\lib\metrics-core-2.2.0.jar;%APP_HOME%\lib\scala-xml_2.11-1.0.4.jar;%APP_HOME%\lib\scala-library-2.11.7.jar;%APP_HOME%\lib\scala-parser-combinators_2.11-1.0.4.jar;%APP_HOME%\lib\jopt-simple-3.2.jar;%APP_HOME%\lib\commons-io-2.4.jar;%APP_HOME%\lib\curator-framework-2.10.0.jar;%APP_HOME%\lib\commons-lang-2.5.jar;%APP_HOME%\lib\json-simple-1.1.jar;%APP_HOME%\lib\reflectasm-1.10.1.jar;%APP_HOME%\lib\minlog-1.3.0.jar;%APP_HOME%\lib\objenesis-2.1.jar;%APP_HOME%\lib\snappy-java-1.1.1.7.jar;%APP_HOME%\lib\lz4-1.2.0.jar;%APP_HOME%\lib\curator-client-2.10.0.jar;%APP_HOME%\lib\asm-5.0.3.jar;%APP_HOME%\lib\zookeeper-3.4.6.jar;%APP_HOME%\lib\guava-16.0.1.jar;%APP_HOME%\lib\jline-0.9.94.jar;%APP_HOME%\lib\netty-3.7.0.Final.jar

@rem Execute kafka-mirror
"%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %KAFKA_MIRROR_OPTS%  -classpath "%CLASSPATH%" com.logitravel.kafka.mirror.Topology %CMD_LINE_ARGS%

:end
@rem End local scope for the variables with windows NT shell
if "%ERRORLEVEL%"=="0" goto mainEnd

:fail
rem Set variable KAFKA_MIRROR_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
if  not "" == "%KAFKA_MIRROR_EXIT_CONSOLE%" exit 1
exit /b 1

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega
