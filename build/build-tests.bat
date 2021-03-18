cd %MSBUILD_TARGET_DIR%
msbuild %MSBUILD_TARGET% /t:Build /p:Configuration=%BUILD_CONFIG% /p:platform=%BUILD_PLATFORM%
%BUILDTOOLS_WIN32%\GitHubStatusNotifier.exe -gitsha:%BUILD_VCS_NUMBER% -successif:%ERRORLEVEL% -context:%BUILD_PLATFORM%,%BUILD_CONFIG% -targeturl:%DUNITX_TARGET_URL%