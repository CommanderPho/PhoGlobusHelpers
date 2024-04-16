# Works in both sh and zsh
if type globus > /dev/null 2>&1; then
    eval "$(globus --completer)"
fi