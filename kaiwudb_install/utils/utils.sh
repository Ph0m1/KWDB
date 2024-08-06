
color_purple='\033[0;35m '
color_red='\033[0;31m '
color_blue='\033[0;34m '
color_green='\033[0;32m '
color_disable=' \033[0m'

export INFO=$color_purple
export PRIMARY=$color_blue
export FAIL=$color_red
export SUCC=$color_green
export NC=$color_disable

function update_config_section() {
  path=$1
  section=$2
  new_section=$3
  if ! test -e $path ; then
    echo "$path not exist!"
    return 1
  fi

  sed -i -e "s/^\[$section\]$/\[$new_section\]/" $path
  return 0
}

function update_config_value() {
  path=$1
  section=$2
  key=$3
  value=$4
  if ! test -e $path ; then
    echo "$path not exist!"
    return 1
  fi
  # update first matched {^$key=} in $section
  # Explanation: /^\[$section\]$/,/^$key=*/ search Range(inclusive)
  # {s/\(^$key=\).*/\1$value/} replace script in Range
  sed -i -e "/^\[$section\]$/,/^$key=*/{s/\(^$key=\).*/\1$value/}" $path
  return 0
}

function delete_config_value() {
  path=$1
  key=$2
  if ! test -e $path ; then
    echo "$path not exist!"
    return 1
  fi
  # delete matched "$key="" in any sections
  sed -i -e "/$key/d" $path
  return 0
}

function read_config_value() {
  path=$1
  section=$2
  key=$3
  if ! test -e $path ; then
    echo "$path not exist!"
    return 1
  fi
  sed -n "/^\[$section\]$/,/^$key=*/{s/^$key=//p}" $path
  return 0
}

# input_me_cfg comment section key defaultvalue
function input_cfg() {
  comment=$1
  section=$2
  key=$3
  value=$(read_config_value $cfgpath $section $key)
  default=${4:-$value}
  echo -e -n "${comment}${INFO}-[$section]:$key:(cur:$value)(def:$default)${NC}:"
  read value
  if [ "$value" = "" ]; then value=$default; fi
  update_config_value $cfgpath $section $key $value
  value=$(read_config_value $cfgpath $section $key)
  echo -e "${SUCC}[$section]:$key:${NC}${value}"
}