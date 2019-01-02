#123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
package Rchen::DBTool;


    ######################################################
    ####          DBTool (SQL DATABASE TOOL)          ####
    ####                                              ####
    ####               Author: Rui Chen               ####
    ####          Contact: ruichen@gmail.com          ####
    ####                                              ####
    ####             All rights reserved.             ####
    ######################################################


#################### How to use this package ####################
# #!/usr/local/bin/perl -w
#
# use lib "/x/home/xin/work/dbtool/current";
#
# # This package is put in the directory "xin"
# # under the path defined by lib
#
# use Rchen::DBTool;
#
# Rchen::DBTool->main();  ### can be called in different ways
#
# 1;
#################################################################


#################### How to call this package ###################

# #1. use the interactive command mode
# Rchen::DBTool->main();

# #2. use command parameter
# #   @ARGV: Database Connect String ([user/password@]database), Statement List
# Rchen::DBTool->cgi_main(@ARGV) if(@ARGV);

# #3. use preset command list, return the results in stdout
# #   cgi_main parameters: Database Connect String, Statement List
# Rchen::DBTool->cgi_main(
#       "DBName1",
#       "SqlStatement1;",
#       "set database DBName2",
#       "prompt SqlStatement2;",
#       "SqlStatement2");

# #4. use preset command list, return the results in an array reference
# #   cgi_return_main parameters: Database Connect String, Statement List
# my $results = Rchen::DBTool->cgi_return_main(
#                     "DBName",
#                     "set display row",
#                     "SqlStatement");
# print @$results;

# #5. use the stdin/stdout
# #   cgi_main parameters: Database Connect String, Statement List
# my (@list, $input);
# while($input = <STDIN>) {
#   chomp($input);
#   push(@list, $input);
# }
# Rchen::DBTool->cgi_main(@list);

#################################################################


use strict;

# for cd command
use Cwd;

# for timing
use Time::Local;

# for color output
use Term::ANSIColor qw(:constants);

# for reading commands
use Term::ReadLine::Zoid;
use POSIX qw(:sys_wait_h);

# for parsing commands
use Text::ParseWords;

# for connecting database
use DBI;

# for getting the user and password
use Rchen::DBPass;

my $term;
my $session = 0;

my %vars;
my %configs;
my %colors;
my %aliases;
my %hidings;
my @query_results;
my @histories;
my @all_tables;
my @all_names;

#######################

## interactive mode main function to get called by outside
## ARG   : None
## RETURN: None
sub main {
  set_defaults();
  tool_info();
  parse_args();
  get_command_shell();
  get_init();
  get_database();
  @histories = get_history();
  add_edit_history();
  get_setting();

  # entering the user mode
  command_loop();
}

#######################

## cgi mode main function to get called by outside
## ARG   : parameter list
## RETURN: 1
sub cgi_main {
  my $self = shift;
  set_defaults();

  cgi_main_internal(@_);
  return 1;
}

#######################

## cgi mode return main function to get called by outside
## ARG   : parameter list
## RETURN: the array reference of the results
sub cgi_return_main {
  my $self = shift;
  set_defaults();

  $vars{return_result} = 1;
  @query_results = ();

  cgi_main_internal(@_);
  return \@query_results;
}

#######################

## cgi mode main internal function
## ARG   : parameter list
## RETURN: None
sub cgi_main_internal {
  my $database = shift;

  $vars{cgi_mode} = 1;

  # disable the dml access in the cgi mode
  $vars{dml_disabled} = 1;

  set_database($database) if $database;
  return if(!$configs{database});

  # entering the user mode
  $vars{user_mode} = 1;

  foreach my $command (@_) {
    if(($configs{redisplay} eq 'on') or
       (($configs{redisplay} eq 'multioff') and
        !is_multiple_command($command))) {
        output("\n$configs{database}: $command\n");
    }
    dispatch_command($command) if $command;
  }

  close_session();
}

## display the tool information
## ARG   : None
## RETURN: None
sub tool_info {
  output(color("\n^\_^  DBTool $vars{VERSION}  ^\_^", BLUE));
  output("\n");
  tool_message();
  output("\n");
}

## display the tool message
## ARG   : None
## RETURN: None
sub tool_message {
}

## parse the command line arguments
## ARG   : None
## RETURN: None
sub parse_args {
  # get the default values
  my $app_path = $0;
  $app_path =~ /(.*\/)/;
  $app_path = $1 || '';

  $vars{INIT_FILE_NAME} = $app_path.$vars{INIT_FILE_NAME};
  $vars{HIST_FILE_NAME} = $vars{TEMP_PATH}.$vars{HIST_FILE_NAME};
  $vars{SET_FILE_NAME}  = $vars{TEMP_PATH}.$vars{SET_FILE_NAME};

  # get options from the command lines
  my $help = 'nohelp';

  use Getopt::Long;
  Getopt::Long::Configure('pass_through');
  GetOptions('init=s'    => \$vars{INIT_FILE_NAME},
             'hist=s'    => \$vars{HIST_FILE_NAME},
             'setting=s' => \$vars{SET_FILE_NAME},
             'db=s'      => \$vars{'init_db'},
             'help:s'    => \$help,
             '<>'        => sub {output("Some options are invalid!\n\n"); &usage});

  if($help ne 'nohelp') {
    if(!$help) {
      &usage;
    }
    else {
      help_command($help, 'usage');
    }
  }
}

## the usage of the tool
## ARG   : None
## RETURN: exit the system
sub usage {
  (my $command = $0) =~ s#.*/##;
  $command =~ s/[0-9]$//;
  select(STDERR); $|=0;
  print "SYNTAX: $command [switches]\n";
  print <<_USAGEM;
  switches are:
  -init=initfile    # set the initialization file
  -hist=histfile    # set the history file
  -setting=setfile  # set the setting file
  -db=initdb        # set the initial database
  -help             # print this message

  -help=command     # list all of valid commands
  -help=about       # list some information about this tool
  -help=doc         # list all of documentation
  -help=init        # explain how to set up the initialization file
  -help=script      # explain how to make a script file
  -help=select      # explain what are the valid select statements
  -help=insert      # explain what are the valid insert statements
  -help=update      # explain what are the valid update statements
  -help=delete      # explain what are the valid delete statements
  -help=multiple    # explain how to make a multiple command
  -help=display     # explain how the display works
  -help=history     # explain how the history works
  -help=setting     # explain how the setting works
  -help=color       # describe what are the valid colors
  -help=variable    # explain how to use variables
  -help=completion  # explain how the name completion works
  -help=alias       # explain how to set up the alias commands
  -help=spool       # explain how to spool the results
  -help=feature     # show the list of features this tool has

_USAGEM
  exit 1;
}

## get the command shell
## ARG   : None
## RETURN: None
sub get_command_shell {
  $term = Term::ReadLine::Zoid->new('DBTool');
  $term->Attribs->{attempted_completion_function} = \&my_name_completion;
}

## set the defaults for the configuration
## ARG   : None
## RETURN: None
sub set_defaults {
  %vars   =('VERSION'        => '6.0',
            'TEMP_PATH'      => '/x/home/'.(getlogin()||getpwuid($<)).'/',
            'INIT_FILE_NAME' => '.dbtool_init',
            'HIST_FILE_NAME' => '.dbtool_hist.'.(getlogin()||getpwuid($<)||$$),
            'SET_FILE_NAME'  => '.dbtool_setting.'.(getlogin()||getpwuid($<)||$$),
            'input_continue' => '',
            'interrupted'    => 0,
            'record_changed' => 0,
            'command_cancel' => 0,
            'command_error'  => 0,
            'not_displayable'=> 0,
            'cgi_mode'       => 0,
            'dml_disabled'   => 0,
            'ddl_disabled'   => 0,
            'db_charset'     => '',
            'output_lines'   => 0,
            'return_result'  => 0,
            'spool_format'   => 'unix',
            'user_mode'      => 0,
            'mysql_connect'  => 0,
            'max_cursors'    => 100,
            'left_bracket'   => '[',
            'right_bracket'  => ']',
            'sort_pref'      => 0,
            'record_list'    => '');

  %configs=('spool'          => 'off',
            'display'        => 'landscape',
            'pause'          => 'nostop',
            'rownum'         => 'off',
            'blankrow'       => 'on',
            'brackets'       => 'on',
            'nondisplayable' => 'on',
            'redisplay'      => 'multioff',
            'spoolformat'    => 'unix',
            'sort'           => 'default',
            'rowsperheading' => 0,
            'heading'        => 'on',
            'savehistory'    => 30,
            'namecompletion' => 'off',
            'columnsize'     => 30,
            'quickshell'     => 'ls|cat|vi|vim|ps|rm|pwd|cd|cp|man|clear|more|less|date|diff');

  %colors =(':EMPHASIS'      => 'blue',
            ':HEADING'       => 'blue',
            ':HISTORY'       => 'on_green',
            ':MESSAGE'       => 'red',
            ':NOTE'          => 'red',

            # corresponding internal colors
            '!:EMPHASIS'     => "\e[$Term::ANSIColor::attributes{blue}m",
            '!:HEADING'      => "\e[$Term::ANSIColor::attributes{blue}m",
            '!:HISTORY'      => "\e[$Term::ANSIColor::attributes{on_green}m",
            '!:MESSAGE'      => "\e[$Term::ANSIColor::attributes{red}m",
            '!:NOTE'         => "\e[$Term::ANSIColor::attributes{red}m");

  %aliases=('db'             => 'set database $1',
            'disp'           => 'set display $1',
            'sysdate'        => 'select sysdate from dual;',
            'tablike'        => 'select table_name from all_tables where table_name '.
                                "like upper('%\$1%');",
            'collike'        => 'select column_name, table_name from all_tab_columns '.
                                "where column_name like upper('%\$1%');");

  $ENV{NLS_LANG} = 'AMERICAN_AMERICA.UTF8';
  $SIG{INT} = \&got_int;

#  $ENV{ORACLE_HOME} = (getpwnam('oracle'))[7] unless defined $ENV{ORACLE_HOME};
#  $ENV{ORACLE_HOME} = '/x/home/oracle/product/8.1.7' unless defined $ENV{ORACLE_HOME};
  $ENV{ORACLE_HOME} = '/x/home/xin/work/dbtool/oracle';
}

## get the user initialization from the $vars{INIT_FILE_NAME} file
## ARG   : None
## RETURN: None
sub get_init {
  if(open(INIT, '<'.$vars{INIT_FILE_NAME})) {
    foreach my $line (<INIT>) {
      chomp($line);
      if(!($line =~ /^ *\#/)) {
        dispatch_command($line);
      }
    }
    close INIT;
  }
}

## get the database name
## ARG   : None
## RETURN: None
sub get_database {
  while(!exists $configs{database}) {
    my $input;
    if($vars{'init_db'}) {
      $input = $vars{'init_db'};
    }
    else {
      $input = get_input_command("\n", '[user/pass@]database: ');
    }
    $input = trim_space($input);

    exit_tool() if($input =~ /^(quit|exit)$/i);

    if($input =~ /^ *(help|\?) *(.*)$/i) {
      help_command($2);
      $input = '';
    }

    set_database($input) if $input;
  }
}

## get the command from input
## ARG  1: The newline string or empty
## ARG  2: The prompt string
## ARG  3: The existing command or none
## RETURN: The command obtained
sub get_input_command {
  my $command;

  if(defined $_[2]) { # existing command
    $command = $_[2];

    if(($configs{redisplay} eq 'on') or
       (($configs{redisplay} eq 'multioff') and
        !is_multiple_command($command))) {
      output($_[0]);
      output(BLUE.$_[1].RESET.$command."\n");
    }
  }
  else {
    output($_[0]);
    spool("$_[1]$command\n") if($command = $term->readline($_[1]));
  }

  return $command;
}

## get the command
## ARG  1: The existing command or none
## RETURN: None
sub get_command {
  my $existing_command = $_[0];

  if($vars{interrupted}) {
    $vars{input_continue} = '';
    $vars{interrupted} = 0;
    return;
  }

  my $command = '';
  if($vars{input_continue}) {
    $command = get_input_command('', S(length($configs{user}.'@'.$configs{database})+2),
                                 $existing_command) || ';';
    $command = $vars{input_continue}.$command;
  }
  else {
    $command = get_input_command("\n", $configs{user}.'@'.$configs{database}.': ', $existing_command);
  }
  $command = trim_space($command);

  # deal with the multiple lines of dml command
  if(is_starting_input_continue($command)) {
    if(is_ending_input_continue($command)) {
      $term->addhistory($command) if($vars{input_continue});
      $vars{input_continue} = '';
    }
    else {
      $vars{input_continue} = $command.' ';
    }
  }
  else {
    $vars{input_continue} = '';
  }

  # get the complete command
  if(!$vars{input_continue}) {
    $vars{output_lines} = 0;
    $vars{interrupted} = 0;
    $vars{command_error} = 0 unless(defined $existing_command);
    $vars{command_cancel} = 0 unless(defined $existing_command);

    my $command_copy = $command;

    # remove comments starting with # or --
    my @output = parse_line('(#|--)+', 1, $command);
    $command = $output[0] || '';
    $command = '' if($command =~ /^[ ;]*$/);

    my $command_copy2 = $command;

    if(!($command =~ /^ *alias +/i)) {
      # get the &param parameter value
      $command =~ s/\\\\/{0x1E}/g;
      $command =~ s/\\\&/{0x1F}/g;

      if(!$vars{cgi_mode}) {
        while($command =~ /\&(.+?)\b/g) {
          output('Input the value of '.E($1).': ');
          my $param = <STDIN>;
          spool($param);
          chomp($param);

          $command =~ s/\&$1/$param/g;
        }
      }

      $command =~ s/{0x1F}/\\\&/g;
      $command =~ s/{0x1E}/\\\\/g;

      # remove the ; symbol
      if(!is_starting_input_continue($command)) {
        $command =~ s/(;*)( *)$//;
      }
    }

    # execute the command
    dispatch_command($command) if $command;

    # check the interrupt
    if($vars{command_cancel}) {
      output("\nUser canceled!\n", $colors{'!:NOTE'}.BOLD);
      $vars{command_cancel} = 0;
    }

    # push to the histories
    if(!defined($existing_command) and !$vars{command_error} and $command_copy2) {
      push_history($command_copy);
    }
  }
}

## handle the commands
## ARG   : None
## RETURN: None
sub command_loop {
  $vars{user_mode} = 1;
  get_command() while(1);
}

## exit the tool
## ARG   : None
## RETURN: exit the system
sub exit_tool {
  # disable changing window title
  if(0) {
    # remove the window title
    if(!$vars{cgi_mode}) {
      print "\033]0;\007";
    }
  }

  output("\nWhen hangs, use \"Ctrl \\\" to exit the dbtool.\n", $colors{'!:NOTE'});

  close SPOOL if($configs{spool} ne 'off');
  $configs{spool} = 'off';

  close_session();
  ###$term->DESTROY if(defined $term);

  # save history and settings before exiting
  save_history();
  save_setting();

  output(color("\n*\_*  Exit DBTool!  *\_*", BLUE));
  output("\n\n");
  exit(1);
}

## dispatch the commands
## ARG   : the command
## RETURN: None
sub dispatch_command {
  my $command = trim_space($_[0]);

  if(is_multiple_command($command)) { # multiple commands
    multiple_command($command);
  }
  elsif($command =~ /^($Rchen::DBPass::DBList)([1-2])?/i) { # database command
    set_database($command);
  }
  elsif($command =~ /^hist(o(r(y)?)?)?;*$/i) { # history
    history_command();
  }
  elsif($command =~ /^([0-9]+?|\/);*$/) { # re-execute history
    reexecute_command($1);
  }
  elsif($command =~ /^!(.*)$/) { # shell
    shell_command($1);
  }
  elsif($command =~ /^($configs{quickshell})( |$)/) {
    shell_command($command);
  }
  elsif($command =~ /^set +/i) { # set
    set_command($command);
  }
  elsif($command =~ /^alias +/i) { # alias
    alias_command($command);
  }
  elsif($command =~ /^color +/i) { # color
    color_command($command);
  }
  elsif($command =~ /^spool +/i) { # spool
    spool_command($command);
  }
  elsif($command =~ /^(@|run ) *(.*)$/i) { # script file
    script_command(trim_space($2));
  }
  elsif($command =~ /^prompt *(.*)$/i) { # prompt
    prompt_command($1);
  }
  elsif($command =~ /^localtime *([^;]*);*$/i) { # localtime
    localtime_command(trim_space($1));
  }
  elsif($command =~ /^(help|\?) *([^;]*);*$/i) { # help
    help_command($2);
  }
  elsif($command =~ /^(exit|quit)$/i) { # exit
    exit_tool();
  }
  elsif($command =~ /^insert +/i) { # insert
    insert_command($command);
  }
  elsif($command =~ /^update +/i) { # update
    update_command($command);
  }
  elsif($command =~ /^delete +/i) { # insert
    delete_command($command);
  }
  elsif($command =~ /^commit */i) { # commit
    commit_command($command);
  }
  elsif($command =~ /^rollback */i) { # rollback
    rollback_command($command);
  }
  elsif($command =~ /^(alter|analyze|create|drop|grant) +/i) { # DDLs
    ddl_command($command);
  }
  elsif($vars{mysql_connect} and
       ($command =~ /^show +(databases|table|index|columns|status|variables|logs)/i)) { # MySQL show
    select_command($command);
  }
  elsif($vars{mysql_connect} and
       ($command =~ /^use +([^ ;]*)/i)) { # MySQL use
    use_command($1);
  }
  elsif($command =~ /^[\( ]*select +/i) { # select
    select_command($command);
  }
  elsif($command =~ /^choose +(\/|[0-9]+)/i) { # choose
    choose_command($command);
  }
  elsif($command =~ /^show +/i) { # show
    show_command($command);
  }
  elsif($command =~ /^hide +/i) { # hide
    hide_command($command);
  }
  elsif($command =~ /^hideall +/i) { # hideall
    hideall_command($command);
  }
  elsif($command =~ /^desc[a-z]* +([^ ;]*)/i) { # desc
    desc_command($1);
  }
  elsif($command =~ /^index +([^ ;]*)/i) { # index
    index_command($1);
  }
  elsif(!$vars{mysql_connect} and
       ($command =~ /^constraint +([^ ;]*)/i)) { # constraint
    constraint_command($1);
  }
  elsif(!$vars{mysql_connect} and
       ($command =~ /^trigger +([^ ;]*)/i)) { # trigger
    trigger_command($1);
  }
  elsif(!$vars{mysql_connect} and
       ($command =~ /^snapshot +([^ ;]*)/i)) { # snapshot
    snapshot_command($1);
  }
  elsif(!$vars{mysql_connect} and
       ($command =~ /^view +([^ ;]*)/i)) { # view
    view_command($1);
  }
  elsif(!$vars{mysql_connect} and
       ($command =~ /^synonym +([^ ;]*)/i)) { # synonym
    synonym_command($1);
  }
  elsif(!$vars{mysql_connect} and
       ($command =~ /^sequence +([^ ;]*)/i)) { # sequence
    sequence_command($1);
  }
  elsif(!$vars{mysql_connect} and
       ($command =~ /^type +([^ ;]*)/i)) { # type
    type_command($1);
  }
  elsif(!$vars{mysql_connect} and
       ($command =~ /^source +([^ ;]*)/i)) { # source
    source_command($1);
  }
  elsif(!$vars{mysql_connect} and
       ($command =~ /^explain +(.*)/i)) { # explain
    explain_command($1);
  }
  elsif(!$vars{mysql_connect} and
       ($command =~ /^session *(.*)/i)) { # session
    session_command($1);
  }
  elsif(!$vars{mysql_connect} and
       ($command =~ /^progress *(.*)/i)) { # progress
    progress_command($1);
  }
  else {
    $command =~ /^(.*?)( |$)/;
    my $alias = $1;
    my $params = $';
    if(exists $aliases{$alias}) { # execute alias command
      execute_alias_command($alias, $params);
    }
    elsif($command) {
      error('Unsupported command in DBTool. Please type '.E('help').' for supported commands.');
    }
  }
}

## handle the select command
## ARG   : the command
## RETURN: None
sub select_command {
  my $start_time = get_epoch_milliseconds();

  my $command = shift;
  $command =~ s/(;| )*$//;
  $command =~ s/\\(.)/$1/g;

  # check if select output will be further processed by shell commands
  # format: select_command DBTOOL_OUTPUT| grep dummy | sort';
  $vars{select_output_command} = '';
  if($command =~ /^(.*[ \t]from[ \t].*)[ \t]dbtool_output/i) {
    $vars{select_output_command} = $';
    $command = $1;

    $vars{return_result_ORIG} = $vars{return_result};
    $vars{return_result} = 1;
    @query_results = ();
  }

  $vars{not_displayable} = 0;

  # Register some SQL commands
  my %cursor_to_sql = (execute_sql => $command);

  my($cursor);
  eval {
    cursor_register(\%cursor_to_sql);
    $cursor = cursor_open('execute_sql', 0);
  };
  return if $vars{interrupted} || check_session_error();

  my $return_value;
  eval {
    $return_value = $cursor->execute(@_);
  };
  return if $vars{interrupted} || check_session_error();

  if(!defined $return_value and $session->{report_errors}) {
    error($session->{dbh}->errstr());
  }

  my $count;
  my $hidden_str;
  if($configs{display} eq 'portrait') {
    ($count, $hidden_str) = display_portrait($cursor);
  }
  elsif($configs{display} eq 'row') {
    ($count, $hidden_str) = display_row($cursor);
  }
  elsif($configs{display} eq 'compare') {
    ($count, $hidden_str) = display_compare($cursor);
  }
  elsif($configs{display} eq 'csv') {
    ($count, $hidden_str) = display_csv($cursor);
  }
  elsif($configs{display} eq 'xml') {
    ($count, $hidden_str) = display_xml($cursor);
  }
  else {
    ($count, $hidden_str) = display_landscape($cursor);
  }

  if($vars{select_output_command}) {
    handle_select_output_command();

    $vars{select_output_command} = '';
    $vars{return_result} = $vars{return_result_ORIG};
    @query_results = ();
  }

  if(defined $count and ($count ne "")) {
    my $time_diff = get_epoch_milliseconds() - $start_time;
    output("\nTotal $count row".(($count>1)?'s':'')." selected (".timing($time_diff).")!\n".
           ($hidden_str?"(Hid $hidden_str column".(($hidden_str =~ /, /)?'s':'').")\n":''),
           $colors{'!:NOTE'}.BOLD);
  }

  eval { $cursor->finish(); };
  return if check_session_error();

  if($vars{not_displayable}) {
    warning('There are nondisplayable characters marked as question marks in the results.');
  }
}

## handle the select output command
## ARG   : None
## RETURN: None
sub handle_select_output_command
{
  my $output = join("", @query_results);
  $output =~ s/\\/\\134/g;
  $output =~ s/"/\\042/g;
  print `echo -e "$output" $vars{select_output_command}`;
}

## handle the insert command
## ARG   : the command
## RETURN: None
sub insert_command {
  my $ret = execute_dml_ddl_command($_[0]);
  output("\nInserted $ret rows!\n", $colors{'!:NOTE'}.BOLD) if($ret != -1);
}

## handle the update command
## ARG   : the command
## RETURN: None
sub update_command {
  my $ret = execute_dml_ddl_command($_[0]);
  output("\nUpdated $ret rows!\n", $colors{'!:NOTE'}.BOLD) if($ret != -1);
}

## handle the delete command
## ARG   : the command
## RETURN: None
sub delete_command {
  my $ret = execute_dml_ddl_command($_[0]);
  output("\nDeleted $ret rows!\n", $colors{'!:NOTE'}.BOLD) if($ret != -1);
}

## handle the commit command
## ARG   : the command
## RETURN: None
sub commit_command {
  my $ret = execute_dml_ddl_command($_[0]);
  output("\nCommit completed!\n", $colors{'!:NOTE'}.BOLD) if($ret != -1);
}

## handle the rollback command
## ARG   : the command
## RETURN: None
sub rollback_command {
  my $ret = execute_dml_ddl_command($_[0]);
  output("\nRollback completed!\n", $colors{'!:NOTE'}.BOLD) if($ret != -1);
}

## handle the ddl commands
## ARG   : the command
## RETURN: None
sub ddl_command {
  execute_dml_ddl_command($_[0]);
}

## handle the mysql desc command
## ARG  1: the table name
## RETURN: None
sub mysql_desc_command {
  my $rows;
  eval {
    $rows = execute_select("desc $_[0]");
  };
  return if(defined $rows and !@$rows);

  my @fields = ('Field', 'Type', 'Null', 'Key', 'Default', 'Extra');
  my %max_len;
  my $heading_text = '';
  my $heading_line = '';
  for my $i (0..5) {
    $max_len{$i} = length($fields[$i]);

    foreach my $row (@$rows) {
      my $len = length(safe(@$row[$i]));
      $max_len{$i} = ($len > $max_len{$i}) ? $len : $max_len{$i};
    }

    $heading_text .= ' '.$fields[$i].S($max_len{$i}-length($fields[$i])).'  ';
    $heading_line .= ' '.pad($max_len{$i}, '-').'--';
  }
  output("$heading_text\n$heading_line\n") if($configs{heading} eq 'on');

  foreach my $row (@$rows) {
    for my $i (0..5) {
      output(' '.safe(@$row[$i]).S($max_len{$i}-length(safe(@$row[$i]))).'  ');
    }
    output("\n");
  }
}

## get the actual table owner and name
## ARG  1: the table owner
## ARG  2: the table name
## RETURN: the actual owner, the actual name
sub get_actual_owner_and_name { 
  my ($owner, $name) = (uc($_[0]), uc($_[1]));

  my $rows = execute_select("select owner from all_objects where object_name = '$name' ".
                            "and object_type = 'SYNONYM' and (owner = 'PUBLIC' ".
                            "or owner = '$owner')");
  my $is_synonym = 0;
  foreach my $row (@$rows) {
    $is_synonym = 1;

    $owner = @$row[0];
    last if($owner ne 'PUBLIC');
  }

  if($is_synonym) {
    my $rows2 = execute_select("select table_owner, table_name from all_synonyms ".
                               "where synonym_name = '$name' and owner = '$owner' and rownum = 1");
    my $row2 = @$rows2[0];
    if($row2) {
      $owner = @$row2[0];
      $name = @$row2[1];
    }
  }

  return ($owner, $name);
}

## handle the desc command
## ARG  1: the table name
## RETURN: None
sub desc_command {
  return mysql_desc_command($_[0]) if($vars{mysql_connect});

  my $owner = '';
  my $table_name = '';

  if($_[0] =~ /\./) {
    $owner = uc($`);
    $table_name = uc($');
  }
  else {
    $table_name = uc($_[0]);
  }

  my ($actual_owner, $actual_name) = get_actual_owner_and_name($owner?$owner:$configs{user}, $table_name);
  my $command = 'select * from (select distinct column_name,nullable,data_type,data_length,'.
                'data_precision,data_scale,column_id '.
                "from all_tab_columns where table_name = '$actual_name' ".
                "and owner = '$actual_owner'".
                ') order by column_id';

  my $rows = execute_select($command);
  if(defined $rows and !@$rows) {
    return error('Table '.E(($owner?($owner.'.'):'').$table_name).' does not exist.');
  }

  output(" Name                            Type             Null?\n");
  output(" ------------------------------  ---------------  --------\n");

  foreach my $row (@$rows) {
    my $type = @$row[2];

    if(($type eq 'NUMBER') and @$row[4]) {
      $type .= '('.@$row[4];
      $type .= ','.@$row[5] if @$row[5];
      $type .= ')';
    }
    elsif(!($type =~ /^(NUMBER|DATE|BLOB|CLOB|LONG|LONG RAW|NCLOB|ROWID|UNDEFINED)$/)) {
      $type .= "(@$row[3])";
    }

    output(' '.(@$row[0]).S(30 - length(@$row[0])).'  '.$type.
           (@$row[1] eq 'Y'? '' : S(17-length($type)).'NOT NULL')."\n");
  }
}

## handle the index command
## ARG  1: the table name
## RETURN: None
sub index_command {
  if($vars{mysql_connect}) {
    return select_command("show index from $_[0];");
  }

  my $owner = '';
  my $table_name = '';

  if($_[0] =~ /\./) {
    $owner = uc($`);
    $table_name = uc($');
  }
  else {
    $table_name = uc($_[0]);
  }

  my ($actual_owner, $actual_name) = get_actual_owner_and_name($owner?$owner:$configs{user}, $table_name);
  my $command = 'select index_name, column_name '.
                "from all_ind_columns where table_name = '$actual_name' ".
                "and table_owner = '$actual_owner' ".
                'order by index_name, column_position';

  my $rows = execute_select($command);
  if(defined $rows and !@$rows) {
    return error('No indexes for table '.E(($owner?($owner.'.'):'').$table_name));
  }

  output(" Index_Name                     Column_Names\n");
  output(" ------------------------------ ---------------------------------------------");

  my $index_name = "";
  foreach my $row (@$rows) {
    if(safe(@$row[0]) ne $index_name) {
      $index_name = safe(@$row[0]);
      output("\n ".$index_name.S(31-length(safe(@$row[0]))).safe(@$row[1]));
    }
    else {
      output(", ".safe(@$row[1]));
    }
  }
  output("\n");
}

## handle the constraint command
## ARG  1: the table name
## RETURN: None
sub constraint_command {
  my $owner = '';
  my $table_name = '';

  if($_[0] =~ /\./) {
    $owner = uc($`);
    $table_name = uc($');
  }
  else {
    $table_name = uc($_[0]);
  }

  my ($actual_owner, $actual_name) = get_actual_owner_and_name($owner?$owner:$configs{user}, $table_name);
  my $command = 'select constraint_name, constraint_type, r_constraint_name, status, search_condition '.
                "from all_constraints where table_name = '$actual_name' ".
                "and owner = '$actual_owner' ".
                'order by constraint_name';

  my $rows = execute_select($command);
  if(defined $rows and !@$rows) {
    return error('No constraints for table '.E(($owner?($owner.'.'):'').$table_name));
  }

  foreach my $row (@$rows) {
    my $constraint_name = safe(@$row[0]);
    my $sub_command = 'select column_name from all_cons_columns '.
                      "where table_name = '$table_name' ".
                      "and constraint_name = '$constraint_name' ";
    $sub_command .= "and owner = '$owner' " if $owner;
    $sub_command .= 'order by position';

    my $cols = execute_select($sub_command);
    if(defined $cols and @$cols) {
      my $column_names = "";
      foreach my $col (@$cols) {
        $column_names .= ($column_names?", ":"").safe(@$col[0]);
      }
      output('       CONSTRAINT_NAME : '.$constraint_name."\n".
             '       CONSTRAINT_TYPE : '.safe(@$row[1])."\n".
             '     R_CONSTRAINT_NAME : '.safe(@$row[2])."\n".
             '                STATUS : '.safe(@$row[3])."\n".
             '          COLUMN_NAMES : '.$column_names."\n".
             '      SEARCH_CONDITION : '.safe(@$row[4])."\n");
    }
    output("\n");
  }
}

## handle the trigger command
## ARG  1: the table name
## RETURN: None
sub trigger_command {
  my $owner = '';
  my $table_name = '';

  if($_[0] =~ /\./) {
    $owner = uc($`);
    $table_name = uc($');
  }
  else {
    $table_name = uc($_[0]);
  }

  my ($actual_owner, $actual_name) = get_actual_owner_and_name($owner?$owner:$configs{user}, $table_name);
  my $command = 'select owner, trigger_name, trigger_type, triggering_event, '.
                'table_owner, base_object_type, table_name, column_name, referencing_names, '.
                'when_clause, status, action_type, description, trigger_body '.
                "from all_triggers where table_name = '$actual_name' ".
                "and table_owner = '$actual_owner'";

  my $rows = execute_select($command);
  if(defined $rows and !@$rows) {
    return error('No triggers for table '.E(($owner?($owner.'.'):'').$table_name));
  }

  foreach my $row (@$rows) {
    output("=============================================================\n".
           '              OWNER : '.safe(@$row[0])."\n".
           '       TRIGGER_NAME : '.safe(@$row[1])."\n".
           '       TRIGGER_TYPE : '.safe(@$row[2])."\n".
           '   TRIGGERING_EVENT : '.safe(@$row[3])."\n".
           '        TABLE_OWNER : '.safe(@$row[4])."\n".
           '   BASE_OBJECT_TYPE : '.safe(@$row[5])."\n".
           '         TABLE_NAME : '.safe(@$row[6])."\n".
           '        COLUMN_NAME : '.safe(@$row[7])."\n".
           '  REFERENCING_NAMES : '.safe(@$row[8])."\n".
           '        WHEN_CLAUSE : '.safe(@$row[9])."\n".
           '             STATUS : '.safe(@$row[10])."\n".
           '        ACTION_TYPE : '.safe(@$row[11])."\n\n".
           "DESCRIPTION : \n".safe(@$row[12])."\n".
           "TRIGGER_BODY : \n".safe(@$row[13])."\n");
  }
}

## handle the snapshot command
## ARG  1: the table name
## RETURN: None
sub snapshot_command {
  my $owner = '';
  my $table_name = '';

  if($_[0] =~ /\./) {
    $owner = uc($`);
    $table_name = uc($');
  }
  else {
    $table_name = uc($_[0]);
  }

  my ($actual_owner, $actual_name) = get_actual_owner_and_name($owner?$owner:$configs{user}, $table_name);
  my $command = 'select owner,master,master_link,refresh_method,name,table_name,last_refresh '.
                "from all_snapshots where table_name='$actual_name' or name='$actual_name' ".
                "and owner = '$actual_owner'";

  my $rows = execute_select($command);
  if(defined $rows and !@$rows) {
    return error('Table '.E(($owner?($owner.'.'):'').$table_name).' is not a snapshot.');
  }

  foreach my $row (@$rows) {
    output(" ================================================\n".
           '          OWNER : '.safe(@$row[0])."\n".
           '         MASTER : '.safe(@$row[1])."\n".
           '    MASTER_LINK : '.safe(@$row[2])."\n".
           ' REFRESH_METHOD : '.safe(@$row[3])."\n".
           '           NAME : '.safe(@$row[4])."\n".
           '     TABLE_NAME : '.safe(@$row[5])."\n".
           '   LAST_REFRESH : '.safe(@$row[6])."\n");
  }
}

## handle the view command
## ARG  1: the table name
## RETURN: None
sub view_command {
  my $owner = '';
  my $table_name = '';

  if($_[0] =~ /\./) {
    $owner = uc($`);
    $table_name = uc($');
  }
  else {
    $table_name = uc($_[0]);
  }

  my ($actual_owner, $actual_name) = get_actual_owner_and_name($owner?$owner:$configs{user}, $table_name);
  my $command = "select owner,view_name,text from all_views where view_name='$actual_name' ".
                "and owner = '$actual_owner'";

  my $rows = execute_select($command);
  if(defined $rows and !@$rows) {
    return error('Table '.E(($owner?($owner.'.'):'').$table_name).' is not a view.');
  }

  foreach my $row (@$rows) {
    my $query = safe(@$row[2]);
    $query =~ s/\"//g;

    output(" ================================================\n".
           '          OWNER : '.safe(@$row[0])."\n".
           '      VIEW_NAME : '.safe(@$row[1])."\n".
           '          QUERY : '.$query."\n");
  }
}

## handle the synonym command
## ARG  1: the synonym name
## RETURN: None
sub synonym_command {
  my $owner = '';
  my $synonym_name = '';

  if($_[0] =~ /\./) {
    $owner = uc($`);
    $synonym_name = uc($');
  }
  else {
    $synonym_name = uc($_[0]);
  }

  my $command = "select owner,synonym_name,table_owner,table_name,db_link from all_synonyms ".
                "where synonym_name='$synonym_name' ".
                "and (owner=upper('".($owner?$owner:$configs{user})."') or owner='PUBLIC')";

  my $rows = execute_select($command);
  if(defined $rows and !@$rows) {
    return error('Synonym '.E(($owner?($owner.'.'):'').$synonym_name).' does not exist.');
  }

  foreach my $row (@$rows) {
    output('          OWNER : '.safe(@$row[0])."\n".
           '   SYNONYM_NAME : '.safe(@$row[1])."\n".
           '    TABLE_OWNER : '.safe(@$row[2])."\n".
           '     TABLE_NAME : '.safe(@$row[3])."\n".
           '        DB_LINK : '.safe(@$row[4])."\n\n");
  }
}

## handle the sequence command
## ARG  1: the sequence name
## RETURN: None
sub sequence_command {
  my $owner = '';
  my $sequence_name = '';

  if($_[0] =~ /\./) {
    $owner = uc($`);
    $sequence_name = uc($');
  }
  else {
    $sequence_name = uc($_[0]);
  }

  my ($actual_owner, $actual_name) = get_actual_owner_and_name($owner?$owner:$configs{user}, $sequence_name);
  my $command = "select sequence_owner,sequence_name,min_value,max_value,increment_by,last_number from all_sequences ".
                "where sequence_name='$actual_name' ".
                "and sequence_owner='$actual_owner'";

  my $rows = execute_select($command);
  if(defined $rows and !@$rows) {
    return error('Sequence '.E(($owner?($owner.'.'):'').$sequence_name).' does not exist.');
  }

  foreach my $row (@$rows) {
    output(' SEQUENCE_OWNER : '.safe(@$row[0])."\n".
           '  SEQUENCE_NAME : '.safe(@$row[1])."\n".
           '      MIN_VALUE : '.safe(@$row[2])."\n".
           '      MAX_VALUE : '.safe(@$row[3])."\n".
           '   INCREMENT_BY : '.safe(@$row[4])."\n".
           '    LAST_NUMBER : '.safe(@$row[5])."\n\n");
  }
}

## handle the type command
## ARG  1: the table name
## RETURN: None
sub type_command {
  my $owner = '';
  my $table_name = '';

  if($_[0] =~ /\./) {
    $owner = uc($`);
    $table_name = uc($');
  }
  else {
    $table_name = uc($_[0]);
  }

  my $command = 'select distinct object_type,owner,count(*) from all_objects '.
                "where object_name='$table_name' ";
  $command .= "and owner = '$owner' " if($owner);
  $command .= 'group by owner,object_type order by owner,object_type';

  my $rows = execute_select($command);
  if(defined $rows and !@$rows) {
    return error('Table '.E(($owner?($owner.'.'):'').$table_name).' does not exist.');
  }

  output(" Type               Owner                          Count\n");
  output(" ------------------ ------------------------------ ------\n");

  foreach my $row (@$rows) {
    output(' '.safe(@$row[0]).S(19-length(safe(@$row[0]))).
           safe(@$row[1]).S(31-length(safe(@$row[1]))).
           safe(@$row[2])."\n");
  }
}

## handle the source command
## ARG  1: the object name
## RETURN: None
sub source_command {
  my $owner = '';
  my $name = '';

  if($_[0] =~ /\./) {
    $owner = uc($`);
    $name = uc($');
  }
  else {
    $name = uc($_[0]);
  }

  my $command = 'select owner, name, type, text '.
                "from all_source where name = '$name'";
  $command .= " and owner = '$owner'" if $owner;

  my $rows = execute_select($command);
  if(defined $rows and !@$rows) {
    return error('Object '.E(($owner?($owner.'.'):'').$name).' does not exist.');
  }

  output(" Owner                          Name                           Type\n");
  output(" ------------------------------ ------------------------------ ------------\n");

  my $row = @$rows[0];
  output(' '.safe(@$row[0]).S(31 - length(safe(@$row[0]))).
         safe(@$row[1]).S(31 -length(safe(@$row[1]))).
         safe(@$row[2])."\n\n") if $row;

  foreach $row (@$rows) {
    output(safe(@$row[3]));
  }
}

## handle the explain command
## ARG  1: The statement to be explained
## RETURN: None
sub explain_command {
  my $sql = $_[0];
  $sql =~ s/(;*)( *)$//;

  if($sql =~ /^ *([0-9]*) *$/) {
    $sql = $histories[$1 - 1]{h};
  }
  elsif($sql =~ /^ *\/ *$/) {
    $sql = $histories[$#histories]{h};
  }

  if(!defined $sql) {
    error('No histories, or history number must between 1 and '.@histories.'.');
    return;
  }

  $sql =~ s/(;*)( *)$//;

  my %cursor_to_sql = (
    explain_plan => q( explain plan set statement_id = '%s' into plan_table for %s),
    read_plan => q( select id, level, position, operation, options,
                    object_name, object_type, optimizer, cost, cardinality, bytes
                    from plan_table
                    connect by prior id = parent_id and statement_id = '%s'
                    start with id = 0 and statement_id = '%s'));

  my $statement_id = sprintf "%d.%d", time, $$;
  $cursor_to_sql{explain_plan} = sprintf $cursor_to_sql{explain_plan}, $statement_id, $sql;
  $cursor_to_sql{read_plan} = sprintf $cursor_to_sql{read_plan}, $statement_id, $statement_id;

  eval {
    cursor_register(\%cursor_to_sql);
    cursor_do('explain_plan');
  };
  return if check_session_error();

  my $rows;
  eval { $rows = aa_for_cursor('read_plan', 0); };
  return if $vars{interrupted} || check_session_error();

  output("\n");
  foreach my $row (@$rows) {
    my $outstr;
    if(@$row[0]) {
      $outstr = sprintf "%2d%s%d.%d ", @$row[1], ' ' x (2 * (@$row[1] - 1)), @$row[1], @$row[2];
      output($outstr);
    }

    $outstr = sprintf "%s %s %s %s", safe(@$row[3]), safe(@$row[4]), safe(@$row[5]), safe(@$row[6]);
    $outstr = lc $outstr;
    $outstr =~ s/\s+/ /g;
    output($outstr);

    if(@$row[8] and @$row[8] > 0) {
      $outstr = sprintf "[cost = %d, cardinality = %.2f, bytes = %12d]",
                         (@$row[8] or 0), (@$row[9] or 0), (@$row[10] or 0);
      $outstr =~ s/\s+/ /g;
      output(" $outstr");
    }
    output("\n");
  }
}

## handle the session command
## ARG  1: the os user name or none
## RETURN: None
sub session_command {
  my $osuser = $_[0] || getlogin() || getpwuid($<);
  if(!$osuser) {
    error('Please give the os user name.');
    return;
  }
  $osuser =~ s/(;*)( *)$//;
  my $command = 'select OSUSER,SID,LOGON_TIME,MACHINE,TERMINAL,PROCESS,MODULE,PROGRAM,STATUS '.
                "from v\$session where osuser = '$osuser' order by logon_time;";

  select_command($command);
}

## handle the progress command
## ARG  1: the number of rows updated per commit
## ARG  2: the os user name or none
## RETURN: None
sub progress_command {
  my $params = $_[0];
  $params =~ /^ *([0-9]*) *([^ ]*)/;
  my $rows_per_commit = $1 || 1;
  if(!$rows_per_commit) {
    error('Please specify the number of rows updated per commit.');
    return;
  }
  my $osuser = $2 || getlogin() || getpwuid($<);
  if(!$osuser) {
    error('Please give the os user name.');
    return;
  }
  $osuser =~ s/(;*)( *)$//;

  my $command = "select s.sid, s.module, ss.value*$rows_per_commit/1000 ||'K' as updates, ".
                "s.username, s.machine, floor((sysdate-s.logon_time)*24)||'h '||".
                "round(mod((sysdate-s.logon_time)*24*60,60),1)||'m' USED_TIME ".
                "from v\$statname sn, v\$sesstat ss, v\$session s ".
                "where sn.statistic\# = ss.statistic\# and ss.sid = s.sid ".
                "and s.osuser = '$osuser' and sn.name = 'user commits' order by s.module, s.sid;";

  select_command($command);
}

## handle the history command
## ARG   : None
## RETURN: None
sub history_command {
  my $fmt = '%'.length(@histories).'s';

  foreach my $i (0..$#histories) {
    output($colors{'!:HISTORY'}) if($i % 2 == 0);
    output('['.sprintf($fmt, $i + 1)."] $histories[$i]{h}".RESET."\n");
  }
}

## handle the <number> command
## ARG  1: The command number
## RETURN: None
sub reexecute_command {
  if(! @histories) {
    output("No histories!\n");
    return;
  }

  my $i;
  if($_[0] eq "\/") {
    $i = $#histories;
  }
  else {
    $i = $_[0] - 1;
  }
  if($i < 0 or $i > $#histories) {
    error('History number must between 1 and '.@histories.".\n");
    return;
  }

  if($histories[$i]{h} =~ /^((\(|\s)*insert|update|delete|commit|rollback)\b/i) {
    output("Do you want to RE-EXECUTE this $1 command (N/Y)? ");

    my $answer = <STDIN>;
    spool($answer);
    chomp($answer);

    get_command($histories[$i]{h}) if($answer =~ /^ *Y/i);
  }
  else {
    if(!$vars{cgi_mode}) {
      get_command($histories[$i]{h});
    }
    else {
      dispatch_command($histories[$i]{h});
    }
  }
}

## handle the shell command
## ARG  1: the shell command
## RETURN: None
sub shell_command {
  return error("Cannot execute any shell command!") if($vars{cgi_mode});

  if($_[0] =~ /^ *cd(.*$|$)/) {
    error('No such directory.') if(! chdir(trim_space($1)));
  }
  else {
    system($_[0]);
  }
}

## get the display size
## ARG   : None
## RETURN: the line/column size array
sub get_display_size
{
  if($ENV{SERVER_PROTOCOL}) {
    # HTTP mode
    return (10000, 10000);
  }
  else {
    # Terminal mode
    return GetTerminalSize();
  }
}

## handle the set command
## ARG   : the command
## RETURN: None
sub set_command {
  my $command = $_[0];

  if($command =~ /^set +linesize +([0-9]+)/i) {
    if($1 > 0) {
      $configs{linesize} = $1;
    }
    else { # if 0, then give the current window size
      $configs{linesize} = (get_display_size)[0] - 2;
    }
  }
  elsif($command =~ /^set +columnsize +([0-9]+)/i) {
    # if 0, then means that the column has no limit
    $configs{columnsize} = $1;
  }
  elsif($command =~ /^set +database +([a-z\-0-9_\/\@]*[a-z\-0-9_]+)/i) {
    set_database($1);
  }
  elsif($command =~ /^set +user +([a-z\-0-9_]+)/i) {
    set_user($1);
  }
  elsif($command =~ /^set +display +(p|l|r|(cs)*v|h|c|x)/i) {
    # change display will affect the blankrow setting
    if(lc($1) eq 'p') {
      $configs{display} = 'portrait';
      $configs{blankrow} = 'on';
    }
    elsif(lc($1) eq 'l') {
      $configs{display} = 'landscape';
      $configs{blankrow} = 'on';
    }
    elsif(lc($1) eq 'r') {
      $configs{display} = 'row';
      $configs{blankrow} = 'off';
    }
    elsif((lc($1) eq 'csv') or (lc($1) eq 'v')) {
      $configs{display} = 'csv';
      $configs{blankrow} = 'off';
      $configs{rowsperheading} = 0;
    }
    elsif(lc($1) eq 'c') {
      $configs{display} = 'compare';
      $configs{blankrow} = 'on';
    }
    elsif(lc($1) eq 'x') {
      $configs{display} = 'xml';
      $configs{blankrow} = 'off';
    }
    else {
      error('Wrong display option. Please type '.E('help').' for commands.');
    }
  }
  elsif($command =~ /^set +pause +(page|record|[0-9]+|nostop)/i) {
    $configs{pause} = lc($1);
  }
  elsif($command =~ /^set +rownum +(off|on)/i) {
    $configs{rownum} = lc($1);
  }
  elsif($command =~ /^set +blankrow +(off|on)/i) {
    $configs{blankrow} = lc($1);
  }
  elsif($command =~ /^set +brackets +(off|on)/i) {
    $configs{brackets} = lc($1);

    if($configs{brackets} eq 'on') {
      $vars{left_bracket} = '[';
      $vars{right_bracket} = ']';
    }
    else {
      $vars{left_bracket} = ' ';
      $vars{right_bracket} = ' ';
    }
  }
  elsif($command =~ /^set +nondisplayable +(off|on)/i) {
    $configs{nondisplayable} = lc($1);
  }
  elsif($command =~ /^set +redisplay +(on|multioff|off)/i) {
    $configs{redisplay} = lc($1);
  }
  elsif($command =~ /^set +spoolformat +(unix|dos|html)/i) {
    $configs{spoolformat} = lc($1);
  }
  elsif($command =~ /^set +sort +(default|column|value) *(asc|desc)*/i) {
    $configs{sort} = lc($1);
    $configs{sort} .= ' '.lc($2) if($2);

    $vars{sort_pref} = get_sorting_preference();
  }
  elsif($command =~ /^set +rowsperheading +([0-9]+)/i) {
    $configs{rowsperheading} = $1;
  }
  elsif($command =~ /^set +heading +(off|on)/i) {
    $configs{heading} = lc($1);
  }
  elsif($command =~ /^set +savehistory +([0-9]+)/i) {
    $configs{savehistory} = $1;
  }
  elsif($command =~ /^set +quickshell +(.+)$/i) {
    $configs{quickshell} = $1;
  }
  elsif($command =~ /^set +namecompletion +(off|on)/i) {
    if($configs{namecompletion} eq 'off' and lc($1) eq 'on') {
      update_all_tables_names();
    }
    if(lc($1) eq 'off') {
      @all_tables = ();
      @all_names = ();
    }

    $configs{namecompletion} = lc($1);
  }
  else {
    error('Wrong set command. Please type '.E('help').' for commands.');
  }
}

## handle the alias command
## ARG   : the command
## RETURN: None
sub alias_command {
  $_[0] =~ /^alias +([a-z\-0-9_]+) */i;
  my $command = $1;
  my $value = trim_space($');

  if($value =~ /^delete$/i) {
    delete $aliases{$command};
  }
  elsif($command and $value) {
    $aliases{$command} = $value;
  }
  else {
    error('Wrong alias command. Please type '.E('help').' for commands.');
  }
}

## handle the show command
## ARG   : the command
## RETURN: None
sub show_command {
  my $command = $_[0];
  my $showall = ($command =~ /^show +all$/i)?1:0;
  my $showed = 0;

  if($showall or $command =~ /^show +setting$/i) {
    foreach my $key (sort(keys %configs)) {
      if(lc($key) eq 'spool') {
        output("$key $configs{$key}\n");
      }
      else {
        output("set $key $configs{$key}\n");
      }
    }

    $showed = 1;
  }

  if($showall or $command =~ /^show +color$/i) {
    foreach my $key (sort(keys %colors)) {
      if(!($key =~ /^!/)) { # !KEY stores the internal color setting
        output("color $key $colors{$key}\n");
      }
    }

    $showed = 1;
  }

  if($showall or $command =~ /^show +alias$/i) {
    my $max_key_len = 0;
    foreach my $key (keys %aliases) {
      $max_key_len = max($max_key_len, length($key));
    }
    foreach my $key (sort(keys %aliases)) {
      output('alias '.E($key).S($max_key_len-length($key))." $aliases{$key}\n");
    }

    $showed = 1;
  }

  if($showall or $command =~ /^show +hiding$/i) {
    foreach my $key (sort(keys %hidings)) {
      output("hide $key $hidings{$key}\n");
    }

    $showed = 1;
  }

  if(!$showed and $command =~ /^show +([a-z_]*)$/i) {
    if(lc($1) eq 'spool') {
      output("spool $configs{spool}\n");
    }
    elsif(defined $configs{lc($1)}) {
      output('set '.lc($1).' '.$configs{lc($1)}."\n");
    }
    else {
      error('No such configuration. '.
            'Please type '.E('help').' for commands.');
    }

    $showed = 1;
  }

  if(!$showed) {
    error('Wrong show command. Please type '.E('help').' for commands.');
  }
}

## handle the hide command
## ARG   : the command
## RETURN: None
sub hide_command {
  if($_[0] =~ /^hide +([^ \t\.]+) +(on|off|delete)$/i) {
    my $name = uc($1);
    my $value = lc($2);

    if($value =~ /^delete$/i) {
      delete $hidings{$name};
    }
    else {
      $hidings{$name} = $value;
    }
  }
  else {
    error('Wrong hide command. Please type '.E('help').' for commands.');
  }
}

## handle the hideall command
## ARG   : the command
## RETURN: None
sub hideall_command {
  if($_[0] =~ /^hideall +(on|off|delete)$/i) {
    my $value = lc($1);

    if($value =~ /^delete$/i) {
      %hidings = ();
    }
    else {
      foreach my $name (keys %hidings) {
        $hidings{$name} = $value;
      }
    }
  }
  else {
    error('Wrong hideall command. Please type '.E('help').' for commands.');
  }
}

## handle the color command
## ARG   : the command
## RETURN: None
sub color_command {
  if($_[0] =~ /^color +(.*?) +(.*?)$/i) {
    my $name = uc($1);
    my $value = lc($2);

    if($value =~ /^delete$/i) {
      if(!($name =~ /^:/)) { # not delete the system colors
        delete $colors{$name};
        delete $colors{'!'.$name};
      }
      else {
        error('Cannot delete the system color.');
      }
    }
    elsif(defined $Term::ANSIColor::attributes{$value}) {
      if($name =~ /^:/) {
        if(!($name =~ /^:(EMPHASIS|HEADING|HISTORY|MESSAGE|NOTE)$/)) {
          error('No such system color setting.');
          return;
        }
      }

      $colors{$name} = $value;
      $colors{'!'.$name} = "\e[$Term::ANSIColor::attributes{$value}m";
    }
    else {
      error('No such color. Please type '.E('help').' for commands.');
    }
  }
  else {
    error('Wrong color command. Please type '.E('help').' for commands.');
  }
}

## handle the spool command
## ARG   : None
## RETURN: None
sub spool_command {
  if($_[0] =~ /^spool +off$/i) {
    if($vars{spool_format} =~ /html/i) {
      $vars{spool_format} = 'unix';
      spool("</PRE></BODY></HTML>");
    }

    $vars{spool_format} = $configs{spoolformat};
    $configs{spool} = 'off';
    close SPOOL;
  }
  else {
    $_[0] =~ /^spool +([^ \t]*)/i;
    my $spool_format = $';

    if($1) {
      if($configs{spool} ne 'off') {
        $configs{spool} = 'off';
        close SPOOL;
      }

      # open the spool file
      if(! open(SPOOL, '>'.$1)) {
        error("Cannot create the spool file: $1.\n");
      }
      else {
        $configs{spool} = $1;
      }

      if($spool_format =~ /^ *(unix|dos|html)/i) {
        $vars{spool_format} = $1;
      }
      else {
        $vars{spool_format} = $configs{spoolformat};
      }

      if($vars{spool_format} =~ /html/i) {
        $vars{spool_format} = 'unix';
        spool("<HTML><HEAD><TITLE>$configs{spool}</TITLE></HEAD><BODY><PRE>");
        $vars{spool_format} = 'html';
      }
    }
    else {
      error('Spool file name is missing.');
    }
  }
}

## handle the multiple commands
## ARG   : the command
## RETURN: None
sub multiple_command {
  my $commands = break_multiple_commands($_[0].';');

  foreach my $one_command (@$commands) {
    return if $vars{interrupted};
    get_command($one_command) if($one_command and !($one_command =~ /^ *$/));
  }
}

## handle the script command
## ARG  1: the script file name
## RETURN: None
sub script_command {
  if(open(SCRIPT, '<'.$_[0])) {
    foreach my $line (<SCRIPT>) {
      if($vars{interrupted}) {
        close SCRIPT;
        return;
      }

      chomp($line);
      get_command($line) if $line;
    }
    close SCRIPT;
  }
  else {
    error("Cannot open the script file: $_[0]");
  }
}

## execute the alias command
## ARG  1: the alias command name
## ARG  2: the parameters string
## RETURN: None
sub execute_alias_command {
  my $command = $aliases{$_[0]};
  my $param;
  my @param_list;

  my @params = parse_line(' +', 1, $_[1]);
  $params[0] = $_[1] if($#params < 0);

  my $cnt = $#params;
  for my $i (0..$cnt) {
    $param = $params[$i];

    if($param) {
      $param =~ s/^('|")//;
      $param =~ s/('|")$//;
      $param =~ s/\\(.)/$1/g;
      push(@param_list, $param) if $param;
    }
  }

  my $i = 0;
  foreach my $param (@param_list) {
    $i++;
    $command =~ s/\$$i/$param/g;
  }

  if($command =~ /\$([0-9]+)/) {
    # have uninitialized params
    error("The param list doesn't match the alias command. ".
          'Please type '.E('help').' for commands.');
  }
  else {
    get_command($command);
  }
}

## execute the select
## ARG  1: the select command
## RETURN: the reference of the result array
sub execute_select {
  # Register some SQL commands
  my %cursor_to_sql = (execute_sql => $_[0]);

  my $rows;
  eval {
    cursor_register(\%cursor_to_sql);
    $rows = aa_for_cursor('execute_sql', 0);
  };
  return if $vars{interrupted} || check_session_error();

  return $rows;
}

## check if the dml can be executed
## ARG   : None
## RETURN: 1 or 0
sub can_execute_dml {
  return 1;
}

## execute the dml or ddl command
## ARG  1: the dml or ddl command
## RETURN: the number of affected rows or none
sub execute_dml_ddl_command {
  my $command = shift;

  if($command=~ /^(insert |delete |update |commit|rollback)/i) {
    if($vars{dml_disabled}) {
      error('The DML (insert, update, delete) access is disabled in this dbtool.');
      return -1;
    }

    if(!can_execute_dml()) {
      error('This dbtool does not allow DML command on this databases.');
      return -1;
    }
  }
  elsif($vars{ddl_disabled}) {
    error('The DDL access is disabled in this dbtool.');
    return -1;
  }

  $command =~ s/(;| )*$//;
  $command =~ s/\\(.)/$1/g;

  # Register some SQL commands
  my %cursor_to_sql = (execute_sql => $command);

  my $rows_affected = 0;
  eval {
    cursor_register(\%cursor_to_sql);
    $rows_affected = cursor_do('execute_sql');
  };

  if($vars{interrupted} || check_session_error()) {
    if(!$configs{user}) {
      warning('You may need to set the privileged user by executing "'.
              E('set user').' <db_user>".');
    }
    return -1;
  }

  if($command =~ /^ *(commit|rollback) *$/i) {
    $vars{record_changed} = 0;
  }
  elsif($rows_affected != '0E0') {
    $vars{record_changed} = 1;
  }
  else {
    $rows_affected = 0;
  }

  return $rows_affected;
}

## change the MySQL database
## ARG  1: the new database name
## RETURN: None
sub use_command {
  if($configs{user} and $configs{database} ne $_[0]) {
    set_database($configs{user}.'@'.$_[0].' mysql');
  }
}

## set the user
## ARG  1: the user name
## RETURN: None
sub set_user {
  if(!$configs{user} or $configs{user} ne $_[0]) {
    if($vars{mysql_connect}) {
      set_database($_[0].'@'.$configs{database}.' mysql');
    }
    else {
      set_database($_[0].'@'.$configs{database});
    }
  }
}

## set the database
## ARG  1: the [user_name/password@]database_name
## RETURN: None
sub set_database {
  # format: [user/password@]database [mysql]
  my $user;
  my $password = '';
  my $database;
  my $str = $_[0];
  $str =~ s/^ *set +database +//i;
  $str = trim_space($str);

  # for cgi mode, format: database
  #if($vars{cgi_mode}) {
  #  if($str =~ /[ @\t]+/) {
  #    return error('Inappropriate database name in this mode (Format: database)!');
  #  }
  #}

  my $new_mysql_connect = 0;
  if($str =~ / +mysql$/i) {
    $new_mysql_connect = 1;
    $str = $`;
  }

  if($str =~ /@/) {
    $user = $`;
    $database = $';
  }
  else {
    $user = '';
    $database = $str;
  }

  if($user =~ /\//) {
    $user = $`;
    $password = $';
  }

  if(!$user) {
    # get the default username and password
    ($database, $user, $password) = &Rchen::DBPass::get_user_pass($database);
  }

  return error('Database is not specified.') if(!$database);
  return error('User must be specified as: user@database.') if(!$user);
  return error('User must be specified for the mysql connection as: user@database mysql.')
  if(!$user and $new_mysql_connect);

  if(!$configs{database} or
     safe($configs{user}).'@'.$configs{database} ne $user.'@'.$database) {
    # ask the password if user is provided
    if($user and !$password) {
      output('Input the user '.E($user).' password: ');
      use Term::ReadKey;
      ReadMode 'noecho';
      $password = ReadLine 0;
      chomp($password);
      ReadMode 'normal';
      output("\n");
    }

    # save history and settings before changing to another database
    save_history();
    save_setting();

    my $newsession = new_session($database,$user,$password,$new_mysql_connect);
    if($newsession) {
      close_session();
      $vars{mysql_connect} = $new_mysql_connect;
      $session = $newsession;

      $configs{database} = $database;
      if($user) {
        $configs{user} = $user;
      }
      else {
        delete $configs{user};
      }
      update_all_tables_names() if($configs{namecompletion} eq 'on');

      # change the session settings
      change_session_settings() if(!$vars{mysql_connect});

      # get the database charset
      $vars{db_charset} = get_database_charset() if(!$vars{mysql_connect});

      # disable changing window title
      if(0) {
        # set the new window title
        if(!$vars{cgi_mode}) {
          print "\033]0;DB:$database\007";
          ###$term->Attribs()->{title} = "DB:$database";
        }
      }
    }
    elsif($configs{database}) {
      return;  # stay the old connection
    }
    else {
      delete $configs{database};
      delete $configs{user};

      if($vars{cgi_mode}) {
        return error("Cannot connect to the database: $database!");
      }
      else {
        get_database();
      }
    }
  }
}

## my completion matches function
## ARG   : None
## RETURN: None
sub my_name_completion {
  my $attribs = $term->Attribs;
  my ($text, $line, $start, $end) = @_;

  if(is_starting_input_continue($vars{input_continue}.$line)) {
    if($configs{namecompletion} eq 'on') {
      # all names
      $attribs->{completion_entry_function} = $attribs->{list_completion_function};
      $attribs->{completion_word} = \@all_names;
    }
  }
  elsif(substr($line,0,$start)=~/^ *(desc([a-z]*)|index|constraint|trigger|snapshot|view|type) /i) {
    if($configs{namecompletion} eq 'on') {
      # all tables
      $attribs->{completion_entry_function} = $attribs->{list_completion_function};
      $attribs->{completion_word} = \@all_tables;
    }
  }
  else {
    # all files
    $attribs->{completion_entry_function} = $attribs->{filename_completion_function};
    $attribs->{completion_word} = \();
  }

  return ();
}

## update all_tables and all_names list
## ARG   : None
## RETURN: None
sub update_all_tables_names {
  output("\n".$colors{'!:NOTE'}.BOLD.'Updating the table and column names. '.
         'This may take a couple of minutes ....'.RESET."\n");

  my $table_command = 'select table_name from all_tables';
  my $column_command = 'select distinct column_name from all_tab_columns';

  @all_tables = ();
  @all_names = ();

  my $table_rows = execute_select($table_command);

  foreach my $row (@$table_rows) {
    push(@all_tables, lc(@$row[0]));
  }
  @all_names = @all_tables;

  my $column_rows = execute_select($column_command);

  foreach my $row (@$column_rows) {
    push(@all_names, lc(@$row[0]));
  }
}

## change the session settings
## ARG   : None
## RETURN: None
sub change_session_settings {
  # change the session date format
  my %cursor_to_sql = (
    nls_date_format => q(alter session set nls_date_format = 'YYYY-MM-DD HH24:MI:SS'));

  eval {
    cursor_register(\%cursor_to_sql);
    cursor_do('nls_date_format');
  };
  return if check_session_error();
}

## get database charset
## ARG   : None
## RETURN: None
sub get_database_charset {
  my $values = execute_select(
      "select value from nls_database_parameters where PARAMETER = 'NLS_CHARACTERSET'");

  foreach my $row (@$values) {
    return (@$row[0]);
  }
}

## check to see if the column is not hidden
## ARG   : the column name
## RETURN: 1 or 0
sub is_not_hidden {
  return 1 if(!($hidings{$_[0]}) or ($hidings{$_[0]} eq 'off'));
  return 0;
}

## display the result in row
## ARG   : None
## RETURN: number of rows and hidden columns, or None
sub display_row {
  my($cursor) = shift @_;

  my $row_num = 0;
  my $display_num = 0;
  my $hidden_str = '';
  my $linesize = get_appropriate_linesize() - 4;
  my $columnsize = $configs{columnsize} || $linesize;

  eval {
    my @columns;
    for (my $ci = 0; $ci < $cursor->{NUM_OF_FIELDS}; $ci++) {
      $columns[$ci] = uc $cursor->{NAME}[$ci];
    }

    # sorting by column
    my $sorted_indexes;
    if($configs{sort} =~ /^column/i) {
      $sorted_indexes = sort_array(\@columns, @columns-0, $vars{sort_pref});
    }

    my (@rows, $result_row, $array_ref);

    my $fetch = 1;
    my $continue = 1;
    my $last_row = undef;
    my $sorted_signature;
    my $temp_sorted_indexes;

    while($fetch) {
      my $num = 0;
      @rows = ();

      # sorting by value
      if(($configs{sort} =~ /^value/i) and defined $last_row) {
        $sorted_indexes = $temp_sorted_indexes;
        $sorted_signature = join('.', @$temp_sorted_indexes);
        push(@rows, $last_row);

        $continue = 1;
        $last_row = undef;
        $num++;
      }

      while($fetch && $continue && ($num<$configs{rowsperheading} or $configs{rowsperheading}==0)) {
        if($array_ref = $cursor->fetch) {
          undef $result_row;
          $result_row = [ @$array_ref ];

          # sorting by value
          if($configs{sort} =~ /^value/i) {
            $temp_sorted_indexes = sort_array($result_row, @columns-0, $vars{sort_pref});

            if($num == 0) {
              $sorted_indexes = $temp_sorted_indexes;
              $sorted_signature = join('.', @$temp_sorted_indexes);
              $continue = 1;
            }
            else {
              if(join('.', @$temp_sorted_indexes) eq $sorted_signature) {
                $continue = 1;
              }
              else {
                $continue = 0;
                $last_row = $result_row;
              }
            }
          }

          if($continue) {
            push(@rows, $result_row);
            $num++;
          }
        }
        else {
          $fetch = 0;
        }
      }

      my @max_col_lens = ();
      # initialize
      for my $i (0..$#columns) {
        $max_col_lens[$i] = 0;
      }

      # get the max length of each field
      foreach my $row (@rows) {
        for my $i (0..$#columns) {
          my $value = handle_nondisplayable(@$row[$i]);

          # solve the max length for multiple lines
          if(($max_col_lens[$i] < $columnsize + 4) and $value) {
            my $len = length($value);
            my $len1 = $len;
            if($value =~ /(\n|\r)/) {
              $len1 = length($`);
            }

            my $max_col;
            if($len1 > $columnsize) {
              $max_col = $columnsize + 4; # +999 or +INF symbols
            }
            elsif($len > $len1) {
              $max_col = $len1 + 4; # +999 or +INF symbols
            }
            else {
              $max_col = $len;
            }

            if($max_col > $max_col_lens[$i]) {
              $max_col_lens[$i] = $max_col;
            }
          }
        }
      }

      # check if need two heading lines
      my $total_len = 0;
      for my $i (0..$#columns) {
        if(is_not_hidden($columns[$i])) { # deal with hidden columns
          $total_len += max($max_col_lens[$i], length($columns[$i])) + 3;
        }
      }

      # assign the right value of max_col_lens
      for my $i (0..$#columns) {
        if($total_len > $linesize) { # two heading lines
          $max_col_lens[$i] = max($max_col_lens[$i], int(length($columns[$i])/2+0.5));
        }
        else { # one heading line
          $max_col_lens[$i] = max($max_col_lens[$i], length($columns[$i]));
        }
      }

      if($num > 0) {
        # display heading in row
        my $heading_str = '';
        my $heading_str2 = '';
        my $current_col = 0;

        for my $i (0..$#columns) {
          my $index = $i;
          if($configs{sort} =~ /^(column|value)/i) {
            $index = @$sorted_indexes[$i];
          }

          my $key = $columns[$index];
          if(is_not_hidden($key)) { # deal with hidden columns
            my $len = $max_col_lens[$index];

            if($total_len > $linesize) {
              # two line heading
              my $key1 = $key;
              my $key2 = '';
              if($len < length($key)) {
                $key1 = substr($key, 0, $len);
                $key2 = substr($key, $len);
              }

              if($len + $current_col > $linesize) {
                $heading_str .= color('+'.form_num(@columns-$i), $colors{'!:NOTE'});
                $heading_str2 .= '';
                last;
              }
              else {
                $heading_str .= color_column($key, $key1.S($len - length($key1)),
                                             $colors{'!:HEADING'}).' | ';
                $heading_str2 .= color_column($key, $key2.S($len - length($key2)),
                                              $colors{'!:HEADING'}).' | ';
                $current_col += $len + 3;
              }
            }
            else {
              # one line heading
              if($len + $current_col > $linesize) {
                $heading_str .= color('+'.form_num(@columns-$i), $colors{'!:NOTE'});
                last;
              }
              else {
                $heading_str .= color_column($key, $key.S($len - length($key)),
                                             $colors{'!:HEADING'}).' | ';
                $current_col += $len + 3;
              }
            }
          }
        }
        if($configs{heading} eq 'on') {
          output("\n$heading_str\n");
          output("$heading_str2\n") if $heading_str2;
        }
        return if $vars{interrupted};
      }

      foreach my $row (@rows) {
        $row_num++;

        # only show the selected records
        next if($vars{record_list} and !($vars{record_list} =~ /:$row_num:/));
        $display_num++;

        if($configs{blankrow} eq 'on') {
          output("\n");
        }

        if($configs{rownum} eq 'on') {
          output("# $row_num #\n");
        }
        return if $vars{interrupted};

        my $current_col = 0;
        for my $i (0..$#columns) {
          my $index = $i;
          if($configs{sort} =~ /^(column|value)/i) {
            $index = @$sorted_indexes[$i];
          }

          my $key = $columns[$index];
          if(is_not_hidden($key)) { # deal with hidden columns
            my $value = safe(handle_nondisplayable(@$row[$index]));

            my $len = length($value);
            if($value =~ /(\n|\r)/) {
              $value = $`;
            }

            if($len > $max_col_lens[$index]) {
              $value = substr($value, 0, $max_col_lens[$index]-4);
            }
            my $len1 = length($value);

            my $truncate = '';
            if($len1 < $len) {
              $truncate = '+'.form_num($len-$len1);
            }

            if($max_col_lens[$index] + $current_col > $linesize) {
              output(color('+'.form_num(@columns-$i), $colors{'!:NOTE'}));
              last;
            }
            else {
              output(color_column($key, $vars{left_bracket}.$value.$vars{right_bracket}).
                     color($truncate, $colors{'!:NOTE'}).
                     S($max_col_lens[$index] - $len1 - length($truncate)).' ');
              $current_col += $max_col_lens[$index] + 3;
            }
            return if $vars{interrupted};
          }
          elsif($display_num == 1) {
            $hidden_str .= $hidden_str?', '.$key:$key;
          }
        }
        output("\n");

        # pause the output
        return if $vars{interrupted};
        if($configs{pause} eq 'record') {
          read_key();
          return if $vars{interrupted};
        }
      }
    }
  };
  return if $vars{interrupted} || check_session_error();

  return $display_num, $hidden_str;
}

## display the result in portrait
## ARG   : None
## RETURN: number of rows and hidden columns, or None
sub display_portrait {
  my($cursor) = shift @_;

  my $row_num = 0;
  my $display_num = 0;
  my $hidden_str = '';
  my $linesize = get_appropriate_linesize() - 2;

  eval {
    my @columns;
    for (my $ci = 0; $ci < $cursor->{NUM_OF_FIELDS}; $ci++) {
      $columns[$ci] = uc $cursor->{NAME}[$ci];
    }

    # sorting by column
    my $sorted_indexes;
    if($configs{sort} =~ /^column/i) {
      $sorted_indexes = sort_array(\@columns, @columns-0, $vars{sort_pref});
    }

    my (@rows, $result_row, $array_ref);

    my $fetch = 1;
    my $continue = 1;
    my $last_row = undef;
    my $sorted_signature;
    my $temp_sorted_indexes;

    while($fetch) {
      my $num = 0;
      @rows = ();

      # sorting by value
      if(($configs{sort} =~ /^value/i) and defined $last_row) {
        $sorted_indexes = $temp_sorted_indexes;
        $sorted_signature = join('.', @$temp_sorted_indexes);
        push(@rows, $last_row);

        $continue = 1;
        $last_row = undef;
        $num++;
      }

      while($fetch && $continue && ($num<$configs{rowsperheading} || $configs{rowsperheading}==0)) {
        if($array_ref = $cursor->fetch) {
          undef $result_row;
          $result_row = [ @$array_ref ];

          # sorting by value
          if($configs{sort} =~ /^value/i) {
            $temp_sorted_indexes = sort_array($result_row, @columns-0, $vars{sort_pref});

            if($num == 0) {
              $sorted_indexes = $temp_sorted_indexes;
              $sorted_signature = join('.', @$temp_sorted_indexes);
              $continue = 1;
            }
            else {
              if(join('.', @$temp_sorted_indexes) eq $sorted_signature) {
                $continue = 1;
              }
              else {
                $continue = 0;
                $last_row = $result_row;
              }
            }
          }

          if($continue) {
            push(@rows, $result_row);
            $num++;
          }
        }
        else {
          $fetch = 0;
        }
      }

      my @max_col_lens = ();
      for my $i (0..$#columns) {
        $max_col_lens[$i] = length($columns[$i]);
      }

      foreach my $row (@rows) {
        for my $i (0..$#columns) {
          my $value = handle_nondisplayable(@$row[$i]);

          # solve the max length for multiple lines
          if(($max_col_lens[$i] < $linesize) and $value) {
            $value =~ /^(.*?)(\n|$)/;
            my $value_len = length($1);

            # maximum heading is a whole line
            $value_len = $linesize if($value_len > $linesize);
            $max_col_lens[$i] = $value_len if($value_len > $max_col_lens[$i]);
          }
        }
      }

      if($num > 0) {
        # display heading in portrait
        my $heading_str = '';
        my $current_col = 0;
        for my $i (0..$#columns) {
          my $index = $i;
          if($configs{sort} =~ /^(column|value)/i) {
            $index = @$sorted_indexes[$i];
          }

          my $key = $columns[$index];
          if(is_not_hidden($key)) { # deal with hidden columns
            my $value = $max_col_lens[$index];

            my $pad_str = '';
            if($value > length($key)) {
              $pad_str = ' '.pad($value - length($key) - 1, '_');
            }

            if($value + $current_col > $linesize) {
              $heading_str .= "\n".color_column($key, $key.$pad_str, $colors{'!:HEADING'}).' | ';
              $current_col = $value + 3;
            }
            else {
              $heading_str .= color_column($key, $key.$pad_str, $colors{'!:HEADING'}).' | ';
              $current_col += $value + 3;
            }
          }
        }
        output("\n$heading_str\n") if($configs{heading} eq 'on');
        return if $vars{interrupted};
      }

      foreach my $row (@rows) {
        $row_num++;

        # only show the selected records
        next if($vars{record_list} and !($vars{record_list} =~ /:$row_num:/));
        $display_num++;

        if($configs{blankrow} eq 'on') {
          output("\n");
        }

        if($configs{rownum} eq 'on') {
          output("# $row_num #\n");
        }
        return if $vars{interrupted};

        my $current_col = 0;
        for my $i (0..$#columns) {
          my $index = $i;
          if($configs{sort} =~ /^(column|value)/i) {
            $index = @$sorted_indexes[$i];
          }

          my $key = $columns[$index];
          if(is_not_hidden($key)) { # deal with hidden columns
            my $value = safe(handle_nondisplayable(@$row[$index]));

            if($max_col_lens[$index] + $current_col > $linesize) {
              output("\n".color_column($key, $vars{left_bracket}.$value.$vars{right_bracket}).
                     S($max_col_lens[$index] - length($value)).' ');
              $current_col = $max_col_lens[$index] + 3;
            }
            else {
              output(color_column($key, $vars{left_bracket}.$value.$vars{right_bracket}).
                     S($max_col_lens[$index] - length($value)).' ');
              $current_col += $max_col_lens[$index] + 3;
            }
            return if $vars{interrupted};

            # deal with the value with newline symbol
            if($value and $value =~ /\n/) {
              output("\n".S($current_col));
            }
          }
          elsif($display_num == 1) {
            $hidden_str .= $hidden_str?', '.$key:$key;
          }
        }
        output("\n");

        # pause the output
        return if $vars{interrupted};
        if($configs{pause} eq 'record') {
          read_key();
          return if $vars{interrupted};
        }
      }
    }
  };
  return if $vars{interrupted} || check_session_error();

  return $display_num, $hidden_str;
}

## display the result in landscape
## ARG   : None
## RETURN: number of rows and hidden columns, or None
sub display_landscape {
  my($cursor) = shift @_;

  my $row_num = 0;
  my $display_num = 0;
  my $hidden_str = '';
  my $linesize = get_appropriate_linesize() - 2;

  my @columns;
  for (my $ci = 0; $ci < $cursor->{NUM_OF_FIELDS}; $ci++) {
    $columns[$ci] = uc $cursor->{NAME}[$ci];
  }

  my $max_key_len = 0;
  for my $i (0..$#columns) {
    if(is_not_hidden($columns[$i])) { # deal with hidden columns
      $max_key_len = max($max_key_len, length($columns[$i]));
    }
  }

  if($max_key_len + 7 >= $linesize) {
    error("The window size is too narrow to display!\n");
    return;
  }

  # sorting by column
  my $sorted_indexes = ();
  if($configs{sort} =~ /^column/i) {
    $sorted_indexes = sort_array(\@columns, @columns-0, $vars{sort_pref});
  }

  eval {
    my ($array_ref, $row);

    while ($array_ref = $cursor->fetch) {
      $row = [ @$array_ref ];
      $row_num++;

      # only show the selected records
      next if($vars{record_list} and !($vars{record_list} =~ /:$row_num:/));
      $display_num++;

      if($configs{blankrow} eq 'on') {
        output("\n");
      }

      if($configs{rownum} eq 'on') {
        output("# $row_num #\n");
      }
      return if $vars{interrupted};

      # sorting by value
      if($configs{sort} =~ /^value/i) {
        $sorted_indexes = sort_array($row, @columns-0, $vars{sort_pref});
      }

      for my $i (0..$#columns) {
        my $index = $i;
        if($configs{sort} =~ /^(column|value)/i) {
          $index = @$sorted_indexes[$i];
        }

        my $key = $columns[$index];

        if(is_not_hidden($key)) { # deal with hidden columns
          my $value = safe(handle_nondisplayable(@$row[$index]));

          output('  '.S($max_key_len-length($key)).color_column($key, $key).
                 ' : '.color_column($key, $vars{left_bracket}.$value.$vars{right_bracket})."\n");
        }
        elsif($display_num == 1) {
          $hidden_str .= $hidden_str?', '.$key:$key;
        }
      }

      # pause the output
      return if $vars{interrupted};
      if($configs{pause} eq 'record') {
        read_key();
        return if $vars{interrupted};
      }
    }
  };
  return if $vars{interrupted} || check_session_error();

  return $display_num, $hidden_str;
}

## display the result in xml
## ARG   : None
## RETURN: number of rows and hidden columns, or None
sub display_xml {
  my($cursor) = shift @_;

  my $row_num = 0;
  my $display_num = 0;
  my $hidden_str = '';

  my @columns;
  for (my $ci = 0; $ci < $cursor->{NUM_OF_FIELDS}; $ci++) {
    $columns[$ci] = uc $cursor->{NAME}[$ci];
  }

  # sorting by column
  my $sorted_indexes = ();
  if($configs{sort} =~ /^column/i) {
    $sorted_indexes = sort_array(\@columns, @columns-0, $vars{sort_pref});
  }

  print "\n";
  eval {
    my ($array_ref, $row);
    while ($array_ref = $cursor->fetch) {
      $row = [ @$array_ref ];
      $row_num++;

      # only show the selected records
      next if($vars{record_list} and !($vars{record_list} =~ /:$row_num:/));
      return if $vars{interrupted};
      $display_num++;

      my $line = '';
      for my $i (0..$#columns) {
        my $index = $i;
        if($configs{sort} =~ /^(column)/i) {
          $index = @$sorted_indexes[$i];
        }

        my $key = $columns[$index];
        if(is_not_hidden($key)) { # deal with hidden columns
          my $value = safe(handle_nondisplayable(@$row[$index]));
          $line .= to_xml_str($key, $value);
        }
        elsif($display_num == 1) {
          $hidden_str .= $hidden_str?', '.$key:$key;
        }
      }
      output("$line\n");

      # pause the output
      return if $vars{interrupted};
      if($configs{pause} eq 'record') {
        read_key();
        return if $vars{interrupted};
      }
    }
  };
  return if $vars{interrupted} || check_session_error();

  return $display_num, $hidden_str;
}

## display the result in csv
## ARG   : None
## RETURN: number of rows and hidden columns, or None
sub display_csv {
  my($cursor) = shift @_;

  my $row_num = 0;
  my $display_num = 0;
  my $hidden_str = '';

  my @columns;
  for (my $ci = 0; $ci < $cursor->{NUM_OF_FIELDS}; $ci++) {
    $columns[$ci] = uc $cursor->{NAME}[$ci];
  }

  # sorting by column
  my $sorted_indexes = ();
  if($configs{sort} =~ /^column/i) {
    $sorted_indexes = sort_array(\@columns, @columns-0, $vars{sort_pref});
  }

  eval {
    my ($array_ref, $row);
    my ($header, $has_header) = ('', 0);
    while ($array_ref = $cursor->fetch) {
      $row = [ @$array_ref ];
      $row_num++;

      # only show the selected records
      next if($vars{record_list} and !($vars{record_list} =~ /:$row_num:/));
      return if $vars{interrupted};
      $display_num++;

      my $line = '';
      my $first_col = 1;
      for my $i (0..$#columns) {
        my $index = $i;
        if($configs{sort} =~ /^(column)/i) {
          $index = @$sorted_indexes[$i];
        }

        my $key = $columns[$index];
        if(is_not_hidden($key)) { # deal with hidden columns
          $header .= ($first_col? '':',').to_csv_str($key) if(!$has_header);

          my $value = safe(handle_nondisplayable(@$row[$index]));
          $line .= ($first_col? '':',').to_csv_str($value);

          $first_col = 0;
        }
        elsif($display_num == 1) {
          $hidden_str .= $hidden_str?', '.$key:$key;
        }
      }
      if(!$has_header) {
        print "\n";
        output("$header\n") if($configs{heading} eq 'on');
        $has_header = 1;
      }
      output("$line\n");

      # pause the output
      return if $vars{interrupted};
      if($configs{pause} eq 'record') {
        read_key();
        return if $vars{interrupted};
      }
    }
  };
  return if $vars{interrupted} || check_session_error();

  return $display_num, $hidden_str;
}

## convert the given string to csv format
## ARG  1: the given string
## RETURN: the csv-formatted string
sub to_csv_str
{
  my $str = shift;

  $str =~ s/"/""/g;
  $str = '"'.$str.'"';

  return $str;
}

## convert the given string to xml format
## ARG  1: the given tag and value
## RETURN: the xml-formatted string
sub to_xml_str
{
  my ($tag, $val) = @_;

  $val =~ s/&/&amp;/g;
  $val =~ s/"/&quot;/g;
  $val =~ s/'/&apos;/g;
  $val =~ s/</&lt;/g;
  $val =~ s/>/&gt;/g;

  return "<$tag>$val</$tag>";
}

## display the buffer data
## ARG  1: the length array reference
## ARG  2: the heading array reference
## ARG  3: the display array reference
## ARG  4: the number of selected columns
## ARG  5: the number of displayed rows
## RETURN: None
sub flush_display {
  my $length_buffer = shift;
  my $heading_buffer = shift;
  my $display_buffer = shift;
  my $col_cnt = shift;
  my $display_rownum = shift;

  if($configs{blankrow} eq 'on') {
    output("\n");
  }

  if($configs{rownum} eq 'on') {
    $col_cnt += 1;
  }

  for my $i (0..$col_cnt-1) {
    # get the maximum display_buffer lines for a column
    my $max_lines = 0;
    for my $j (0..$display_rownum-1) {
      my $tmp_buffer = @$display_buffer[$j*$col_cnt+$i];
      my $lines = @$tmp_buffer;

      $max_lines = max($max_lines, $lines);
    }

    for my $k (0..$max_lines-1) {
      my $heading = @$heading_buffer[$i];
      $heading = S(@$length_buffer[0]) if($k);
      output($heading);

      for my $j (0..$display_rownum-1) {
        my $tmp_buffer = @$display_buffer[$j*$col_cnt+$i];

        my $tmp = safe(@$tmp_buffer[$k]);
        $tmp = ($j?' | ':' : ').' '.S(@$length_buffer[$j+1]-5).' ' if(!$tmp);
        output($tmp);
      }
      output("\n");
    }
  }
}

## choose the selected records of a history command
## ARG  1: the history command
## ARG  2: the selected record list
## RETURN: None
sub choose_command {
  my $command = shift;
  $command =~ /^choose +(\/|[0-9]+)/i;

  # deal with the selected records
  my $record_list = $';
  $vars{record_list} = '';
  while($record_list =~ /([0-9]+)/g) {
    $vars{record_list} .= ":$1:";
  }

  reexecute_command($1);
  $vars{record_list} = '';
}

## display the result in compare mode
## ARG   : None
## RETURN: number of rows and hidden columns, or None
sub display_compare {
  my($cursor) = shift @_;

  my $row_num = 0;
  my $display_num = 0;
  my $hidden_str = '';
  my $hidden_num = 0;
  my $linesize = get_appropriate_linesize() - 2;

  my @columns;
  for (my $ci = 0; $ci < $cursor->{NUM_OF_FIELDS}; $ci++) {
    $columns[$ci] = uc $cursor->{NAME}[$ci];
  }

  my $max_key_len = 0;
  for my $i (0..$#columns) {
    if(is_not_hidden($columns[$i])) { # deal with hidden columns
      $max_key_len = max($max_key_len, length($columns[$i]));
    }
  }
  $max_key_len = max($max_key_len, 11) if($configs{rownum} eq 'on');
  my $columnsize = $configs{columnsize} || ($linesize-$max_key_len-7);

  if($max_key_len + 7 + $columnsize > $linesize) {
    error("The window is too narrow or the columnsize is too big ".
          "to display in the compare mode!\n");
    return;
  }

  # sorting by column
  my $sorted_indexes = ();
  if($configs{sort} =~ /^column/i) {
    $sorted_indexes = sort_array(\@columns, @columns-0, $vars{sort_pref});
  }

  my @heading_buffer = ();
  my @display_buffer = ();
  my @length_buffer = ();
  my $display_buffer_rownum = 0;
  my $current_len = $max_key_len + 2;

  eval {
    my ($array_ref, $row);

    if($configs{rownum} eq 'on') {
      push(@heading_buffer, S(2+$max_key_len-11)."# row num #");
    }

    while ($array_ref = $cursor->fetch) {
      $row = [ @$array_ref ];

      $row_num++;
      return if $vars{interrupted};

      # only show the selected records
      next if($vars{record_list} and !($vars{record_list} =~ /:$row_num:/));
      $display_num++;

      # get the max_value_len
      my $max_value_len = 0;
      for my $i (0..$#columns) {
        if(is_not_hidden($columns[$i])) { # deal with hidden columns
          $max_value_len = max($max_value_len, length(safe(@$row[$i])));
        }
      }
      $max_value_len = min($max_value_len, $columnsize);

      if($current_len + $max_value_len + 5 > $linesize) {
        # display the current display buffer
        flush_display(\@length_buffer, \@heading_buffer, \@display_buffer,
                      $#columns+1-$hidden_num, $display_buffer_rownum);

        @display_buffer = ();
        @length_buffer = ();
        $display_buffer_rownum = 0;
        $current_len = $max_key_len + 2;
      }

      if($configs{rownum} eq 'on') {
        my $row_num_str = "# $row_num #";
        $row_num_str = "#$row_num#" if($columnsize < 3);
        my $spaces = int(($max_value_len-length($row_num_str))/2);

        my @buffer = ();
        push(@buffer, ($display_buffer_rownum?' | ':' : ').S(1+$spaces).$row_num_str.
                      S($max_value_len-$spaces-length($row_num_str)+1));
        push(@display_buffer, \@buffer);
      }

      # set the length_buffer
      push(@length_buffer, 2+$max_key_len) if($display_buffer_rownum == 0);
      push(@length_buffer, 5+$max_value_len);

      # add the current row into the display buffer
      for my $i (0..$#columns) {
        my $index = $i;
        if($configs{sort} =~ /^column/i) {
          $index = @$sorted_indexes[$i];
        }

        my $key = $columns[$index];
        if(is_not_hidden($key)) { # deal with hidden columns
          # create the heading buffer
          if($display_num == 1) {
            push(@heading_buffer, S(2+$max_key_len-length($key)).color_column($key, $key));
          }

          my $value = safe(handle_nondisplayable(@$row[$index]));

          # break the value to be right segments
          my $segments = break_string($value, $max_value_len);
          my @buffer = ();

          my $left;
          my $right;
          foreach my $s (0..@$segments-1) {
            my $seg = @$segments[$s];

            if(@$segments > 1) {
              if($configs{brackets} eq 'on') {
                $left = '+';
                $right = '+';
              }
              else {
                $left = ' ';
                $right = ' ';
              }

              if($s == 0) {
                $left = $vars{left_bracket};
              }
              elsif($s == @$segments-1) {
                $right = $vars{right_bracket};
              }
            }
            else {
              $left = $vars{left_bracket};
              $right = $vars{right_bracket};
            }

            push(@buffer, ($display_buffer_rownum?' | ':' : ').
                           color_column($key, $left.$seg.$right.S($max_value_len-length($seg))));
          }
          push(@display_buffer, \@buffer);
        }
        elsif($display_num == 1) {
          $hidden_str .= $hidden_str?', '.$key:$key;
          $hidden_num++;
        }
      }

      $display_buffer_rownum++;
      $current_len += $max_value_len + 5;

      # pause the output
      return if $vars{interrupted};
      if($configs{pause} eq 'record') {
        read_key();
        return if $vars{interrupted};
      }
    }

    # display the remaining in the buffer
    flush_display(\@length_buffer, \@heading_buffer, \@display_buffer,
                  $#columns+1-$hidden_num, $display_buffer_rownum) if(@display_buffer);
  };
  return if $vars{interrupted} || check_session_error();

  return $display_num, $hidden_str;
}

## break string into segments according to specified length
## ARG  1: the string
## ARG  2: the max segment length
## RETURN: the reference of the segment array
sub break_string {
  my $str = $_[0];
  my $seg_len = $_[1];

  my @segs = ();
  push(@segs, '') if(!(defined $str) or !length($str));

  while(length($str)) {
    my $piece;
    if($str =~ /\n/) {
      $piece = $`;
      $str = $';
    }
    else {
      $piece = $str;
      $str = '';
    }

    if(length($piece) >= $seg_len) {
      push(@segs, substr($piece, 0, $seg_len));
      $str = substr($piece, $seg_len).(length($str)?"\n".$str:'');
    }
    else {
      push(@segs, $piece);
    }
  }

  return \@segs;
}

## check the database error
## ARG   : None
## RETURN: 1 or 0
sub check_session_error {
  my ($err, $errstr, $state);

  # Perform error handling
  eval { ($err, $errstr, $state) = check_error(); };

  if($err) {
    error("\n- $errstr - $err");
    return 1;
  }

  # no error
  return 0;
}

## close session
## ARG   : None
## RETURN: None
sub close_session {
  if($session) {
    # rollback/commit changes before closing the session
    if($vars{record_changed}) {
      output("Do you want to COMMIT your uncommitted changes in $configs{database} (N/Y)? ");

      my $answer = <STDIN>;
      spool($answer);
      chomp($answer);

      if($answer =~ /^ *Y/i) {
        execute_dml_ddl_command('commit');
      }
      else {
        execute_dml_ddl_command('rollback');
      }
    }

    $session->{cursors} = undef;
    DESTROY_SESSION();
  }
  $session = 0;
}

## padding to align the output
## ARG  1: the number of padding times
## ARG  2: the string to be padded
## RETURN: the result string
sub pad {
  my $pad_str = '';

  for(my $i = 0; $i < $_[0]; $i++) {
    $pad_str .= $_[1];
  }

  return $pad_str;
}

## make the number to be right format
## ARG  1: the number
## RETURN: the right format number string: 999|99K|.9M|99M|.9G|99G|INF
sub form_num {
  my $num = $_[0];

  if($num < 1000) {
    return $num;
  }
  else {
    $num = int($num/1000+0.5);
    if($num < 100) {
      return $num.'K';
    }
    elsif($num < 1000) {
      return '.'.int($num/100+0.5).'M';
    }
    else {
      $num = int($num/1000+0.5);
      if($num < 100) {
        return $num.'M';
      }
      elsif($num < 1000) {
        return '.'.int($num/100+0.5).'G';
      }
      else {
        $num = int($num/1000+0.5);
        if($num < 100) {
          return $num.'G';
        }
        else {
          return 'INF';
        }
      }
    }
  }
}

## sort the array
## ARG  1: the reference of the array to be sorted
## ARG  2: the size of the array
## ARG  3: the sorting preference: 0 -- asc or 1 -- desc
## RETURN: the reference of the sorted index array
sub sort_array {
  my $sort_array = $_[0];
  my $array_size = $_[1] - 1;  # starting from 0
  my $desc = $_[2];

  my @index_array;
  for my $i (0..$array_size) {
    $index_array[$i] = $i;
    @$sort_array[$i] = '' if(!defined @$sort_array[$i]);
  }

  my $tmp_index;
  for my $i (0..$array_size-1) {
    $tmp_index = $i;
    for my $j ($i+1..$array_size) {
      if(@$sort_array[$index_array[$tmp_index]] lt @$sort_array[$index_array[$j]]) {
        $tmp_index = $j if $desc;
      }
      else {
        $tmp_index = $j if !$desc;
      }
    }

    if($tmp_index != $i) {
      my $tmp = $index_array[$i];
      $index_array[$i] = $index_array[$tmp_index];
      $index_array[$tmp_index] = $tmp;
    }
  }

  return \@index_array;
}

## get the maximum value
## ARG  1: the first number
## ARG  2: the second number
## RETURN: the bigger number
sub max {
  return ($_[0]>$_[1])?$_[0]:$_[1];
}

## get the minimum value
## ARG  1: the first number
## ARG  2: the second number
## RETURN: the smaller number
sub min {
  return ($_[0]<$_[1])?$_[0]:$_[1];
}

## get the safe string
## ARG  1: the input string
## RETURN: the safe output string
sub safe {
  return (defined $_[0])?$_[0]:'';
}

## handle the nondisplayable string
## ARG  1: the input string
## RETURN: the displayable output string
sub handle_nondisplayable {
#  if(($configs{nondisplayable} eq 'off')) { # and ($vars{db_charset} eq 'UTF8')) {
#    if(defined $_[0]) {
#      use utf8;
#      my $str = $_[0];
#      if($str =~ s/([\x{80}-\x{FFFFFFFF}])/?/g) {
#        $vars{not_displayable} = 1;
#      }
#      return $str;
#    }
#  }

  return $_[0];
}

## trim the edge spaces
## ARG  1: the regular string
## RETURN: the string without spaces in the ends
sub trim_space {
  return '' if(!$_[0]);

  my $trim_str = $_[0];
  $trim_str =~ s/^\s+//;
  $trim_str =~ s/\s+$//;

  return $trim_str;
}

## timing message
## ARG  1: the time difference in milliseconds
## RETURN: the string with timing format
sub timing {
  my $d = shift;

  my $ms = $d%1000;
  $d = int($d/1000);

  my $s = $d%60;
  $d = int($d/60);

  my $m = $d%60;
  my $h = int($d/60);

  return ($h?$h." hour".($h>1?"s ":" "):"").
         (($m||$h)?$m." minute".($m>1?"s ":" "):"").
         $s.($ms?'.'.pad(3-length($ms), '0').$ms:'')." second".($s>1?"s":"");
}

## get the appropriate linesize
## ARG   : None
## RETURN: the appropriate linesize
sub get_appropriate_linesize {
  my $linesize;

  if(!$configs{linesize}) { # automatically set the best display size
    $linesize = (get_display_size())[0] - 2;
  }
  elsif($configs{linesize} > (get_display_size())[0]) {
    $linesize = (get_display_size())[0] - 2;
  }
  else {
    $linesize = $configs{linesize};
  }

  return $linesize;
}

## get the sorting preference
## ARG   : None
## RETURN: the sorting preference
sub get_sorting_preference {
  my $sorting;

  if($configs{sort} =~ / desc$/i) {
    $sorting = 1; # desc
  }
  elsif($configs{sort} =~ / asc$/i) {
    $sorting = 0; # asc
  }
  else {
    if($configs{sort} =~ /^column/i) {
      $sorting = 0; # default asc
    }
    elsif($configs{sort} =~ /^value/i) {
      $sorting = 1; # default desc
    }
    else {
      $sorting = 0;
    }
  }

  return $sorting;
}

## display color column value
## ARG  1: the column name
## ARG  2: the column value string
## ARG  3: the default color or none
## RETURN: the colored column value string
sub color_column {
  if(exists $colors{'!'.uc($_[0])}) {
    return $colors{'!'.uc($_[0])}.$_[1].RESET;
  }
  elsif(defined $_[2]) {
    return $_[2].$_[1].RESET;
  }
  else {
    return $_[1];
  }
}

## display color string
## ARG  1: the string
## ARG  2: the color
## RETURN: the colored string
sub color {
  return $_[1].$_[0].RESET;
}

## output control
## ARG  1: a regular string
## RETURN: None
sub output_control {
  if($vars{return_result}) {
    push(@query_results, $_[0]);
    return;
  }

  if(($configs{pause} ne 'nostop') and ($configs{pause} ne 'record')) {
    # pause the output
    my $colsize = 0;
    my $linesize = (get_display_size())[0];

    if($configs{pause} eq 'page') {
      $colsize = (get_display_size())[1] - 4;
    }
    elsif($configs{pause} =~ /([0-9]+)/) {
      $colsize = $1;
    }

    my $str = $_[0];
    while($str) {
      return if $vars{interrupted};

      if($str =~ /\n/) {
        print $`."\n";
        $str = $';

        $vars{output_lines} += 1 + int(length($`)/$linesize);
        if($colsize > 0 and $vars{output_lines} >= $colsize) {
          read_key();
          $vars{output_lines} = 0;
        }
      }
      else {
        print $str;
        $str = '';
        $vars{output_lines} += int(length($str)/$linesize);
      }
    }
  }
  else {
    print $_[0];
  }
}

## read a key
## ARG   : None
## RETURN: None
sub read_key {
  return if($vars{cgi_mode});

  use Term::ReadKey;
  ReadMode('cbreak');

  print $colors{'!:NOTE'}."\nPress any key to continue ....".RESET."\n";
  ReadKey(0);

  ReadMode('normal');
}

## add the history commands into the term history
## ARG   : None
## RETURN: None
sub add_edit_history
{
  foreach my $i (0..$#histories) {
    $term->addhistory($histories[$i]{h});
  }
}

## push the command into histories
## ARG  1: the history string
## RETURN: None
sub push_history {
  my $hist = $_[0];
  return if($_[0] =~ /^ *(commit|rollback)\b/i);

  # only push history for dml command or multiple command
  if(is_dml_command($hist) or is_multiple_command($hist)) {
    if((@histories == 0) or ($histories[@histories - 1]{h} ne $hist)) {
      push(@histories, {t=>get_epoch_seconds(), h=>$hist});
    }
  }
}

## get history
## ARG   : None
## RETURN: The history array
sub get_history {
  my @tmp_histories = ();

  if(!$vars{cgi_mode} and open(HIST, '<'.$vars{HIST_FILE_NAME})) {
    foreach my $line (<HIST>) {
      chomp($line);
      $line =~ /^([0-9]*):(.*)$/;

      push(@tmp_histories, {t=>$1, h=>$2}) if ($1 && $2);
    }
    close HIST;
  }

  return @tmp_histories;
}

## save history
## ARG   : None
## RETURN: None
sub save_history {
  return if($vars{cgi_mode});

  if($vars{user_mode} and ($configs{savehistory} > 0) and @histories) {
    # get the latest histories from the file then combine them together
    my @tmp_histories = @histories;
    push(@tmp_histories, get_history());
    @tmp_histories = sort { $a->{t} <=> $b->{t} } (@tmp_histories);

    if(open(HIST, '>'.$vars{HIST_FILE_NAME}.".tmp")) {
      # remove the least recent duplicate entries and those with number or / command inside
      my %seen = ();

      foreach my $i (0..$#tmp_histories) {
        my $hist = $tmp_histories[$i]{h};
        my $commands = break_multiple_commands($hist);

        my $save = 1;
        foreach my $command (@$commands) {
          if($command and ($command =~ /^ *(([0-9]+)|\/) *;? *$/)) {
            $save = 0;
            last;
          }
        }

        $seen{$hist} = $i if($save);
      }
      my @saved_history_indexes = sort { $a <=> $b } (values %seen);

      # save the history commands
      my $start_from = $#saved_history_indexes + 1 - $configs{savehistory};
      $start_from = 0 if $start_from < 0;

      for my $i ($start_from..$#saved_history_indexes) {
        print HIST $tmp_histories[$saved_history_indexes[$i]]{t}.':'.
                   $tmp_histories[$saved_history_indexes[$i]]{h}."\n";
      }

      close HIST;

      # for safety
      `mv $vars{HIST_FILE_NAME}.tmp $vars{HIST_FILE_NAME}`;
    }
  }
}

## get setting
## ARG   : None
## RETURN: None
sub get_setting {
  if(!$vars{cgi_mode} and open(SET, '<'.$vars{SET_FILE_NAME})) {
    foreach my $line (<SET>) {
      chomp($line);
      dispatch_command($line);
    }

    close SET;
  }
}

## save settings
## ARG   : None
## RETURN: None
sub save_setting {
  return if($vars{cgi_mode});

  if($vars{user_mode} and open(SET, '>'.$vars{SET_FILE_NAME}.".tmp")) {
    while(my($key, $value) = each %configs) {
      if(lc($key) eq 'spool') {
        # print SET "$key $configs{$key}\n";   # don't save this setting
      }
      # don't save the database and user setting, namecompletion, linesize
      elsif(!($key =~ /^(database|user|namecompletion|linesize)$/i)) {
        print SET "set $key $configs{$key}\n";
      }
    }

    while(my($key, $value) = each %colors) {
      if(!($key =~ /^!/)) { # not save internal colors
        print SET "color $key $colors{$key}\n";
      }
    }

    while(my($key, $value) = each %hidings) {
      print SET "hide $key $hidings{$key}\n";
    }

    while(my($key, $value) = each %aliases) {
      print SET "alias $key $aliases{$key}\n";
    }

    close SET;

    # for safety
    `mv $vars{SET_FILE_NAME}.tmp $vars{SET_FILE_NAME}`;
  }
}

## break multiple commands by semicolon
## ARG  1: a multiple command string
## RETURN: a reference of the command array
sub break_multiple_commands {
  my @commands = parse_line(';+', 1, $_[0]);
  $commands[0] = $_[0] if($#commands < 0);

  my @results;
  my $command;
  my $cnt = $#commands;
  for my $i (0..$cnt) {
    $command = $commands[$i];

    $command .= ';' if($i < $cnt);
    push(@results, $command);
  }

  return \@results;
}

## check if it's starting a dml command
## ARG  1: a command string
## RETURN: 1 or 0
sub is_starting_input_continue {
  my $commands = break_multiple_commands($_[0]);
  my $last_command = @$commands[@$commands - 1];
  return 0 unless (defined $last_command);

  return ($last_command =~ /^((\(|\s)*select|insert|update|delete|explain +select)\b/i);
}

## check if it's ending a dml command
## ARG  1: a command string
## RETURN: 1 or 0
sub is_ending_input_continue {
  my $commands = break_multiple_commands($_[0]);
  my $last_command = @$commands[@$commands - 1];
  return 1 unless (defined $last_command);

  if(is_starting_input_continue($last_command)) {
    $last_command =~ s/\\\\//g;
    return ($last_command =~ /[^\\];$/);   # not completely right, such as ['asdasf;]
  }
  return 1;
}

## check if it's a dml command
## ARG  1: a command string
## RETURN: 1 or 0
sub is_dml_command {
  return ($_[0] =~ /^((\(|\s)*select|insert|update|delete)\b/i);
}

## check if it's a multiple command
## ARG  1: a command string
## RETURN: 1 or 0
sub is_multiple_command {
  return 0 if($_[0] =~ /^alias +/i);

  $_[0] =~ /(;| )*$/;
  my $commands = break_multiple_commands($`);
  return (@$commands > 1);
}

## output the results
## ARG  1: the output string
## ARG  2: the color or none
## ARG  3: if spool string
## RETURN: None
sub output {
  my $str = $_[0];

  if($vars{cgi_mode}) {
    # remove color if cgi mode
    $str =~ s/\e\[([0-9]+)m//g;
    output_control($str);
  }
  elsif(defined $_[1]) {
    print $_[1];
    output_control($str);
    print RESET;
  }
  else {
    output_control($str);
  }

  spool($str) unless (defined $_[2]);
}

## spool the output
## ARG  1: the spool string
## RETURN: None
sub spool {
  if($configs{spool} ne 'off') {
    my $str = $_[0];
    $str =~ s/\e\[([0-9]+)m//g;

    if($vars{spool_format} =~ /dos/i) {
      $str =~ s/\n/\r\n/g;
    }
    elsif($vars{spool_format} =~ /html/i) {
      $str =~ s/\n/\r\n/g;
      $str =~ s/\&/\&amp;/g;
      $str =~ s/\</\&lt;/g;
      $str =~ s/\>/\&gt;/g;
    }

    print SPOOL $str;
  }
}

## display error messages
## ARG  1: the error string
## RETURN: None
sub error {
  $vars{command_error} = 1;

  output('Error:', $colors{'!:NOTE'}.BOLD);
  output(" $_[0]\n");
}

## display warning messages
## ARG  1: the warning string
## RETURN: None
sub warning {
  output('Warning:', $colors{'!:NOTE'}.BOLD);
  output(" $_[0]\n");
}

## trap the INT
## ARG   : None
## RETURN: None
sub got_int {
  $vars{interrupted} = 1;
  $vars{command_cancel} = 1;
}

## handle the prompt command
## ARG  1: the prompt string
## RETURN: None
sub prompt_command {
  my  $str = shift;
  $str =~ s/\\(.)/$1/g;
  output("$str\n") if $str;
}

## get the epoch seconds of the current time
## ARG  1: None
## RETURN: the epoch seconds
sub get_epoch_seconds {
  return timelocal( (localtime)[0,1,2,3,4,5]);
}

## get the epoch milliseconds of the current time
## ARG  1: None
## RETURN: the epoch milliseconds
sub get_epoch_milliseconds {
  chomp(my $second = `date +"%s.%N"`);
  $second =~ s/%N/000/;
  return int($second*1000);
}

## handle the localtime command
## ARG  1: The specified timezone or none
## RETURN: None
sub localtime_command {
  my $utc_time = `date -u +"%Y-%m-%d %H:%M:%S"`;
  chomp($utc_time);

  my $original_time_zone = $ENV{TZ};
  my ($epoch_seconds,$date,$time,$mon,$day,$year,$hour,$min,$sec);

  my @output_time_zones;
  if($_[0]) {
    @output_time_zones = ($_[0]);
  }
  else {
    @output_time_zones=('UTC','US/Pacific','Europe/London','Europe/Berlin','Europe/Paris','Japan');
  }

  # convert the utc time to be the epoch seconds
  ($date,$time) = split(/ /, $utc_time);
  ($year,$mon,$day) = split(/-/, $date);
  ($hour,$min,$sec) = split(/:/, $time);

  $ENV{TZ} = 'UTC';
  eval { $epoch_seconds = timelocal($sec,$min,$hour,$day,$mon-1,$year-1900); };
  if($@) {
    error('Cannot convert the current utc time to epoch seconds.');
    return;
  }

  # convert the epoch seconds to be the output time
  foreach my $output_time_zone (@output_time_zones) {
    $ENV{TZ} = $output_time_zone;
    eval { ($sec,$min,$hour,$day,$mon,$year) = localtime($epoch_seconds); };
    if($@) {
      error('Cannot convert the epoch seconds to the local time');
      return;
    }
    $mon += 1;
    $year += 1900;

    $sec = '0'.$sec if($sec < 10);
    $min = '0'.$min if($min < 10);
    $hour = '0'.$hour if($hour < 10);
    $day = '0'.$day if($day < 10);
    $mon = '0'.$mon if($mon < 10);

    output("  Time: $year-$mon-$day $hour:$min:$sec   Zone: $output_time_zone\n");
  }

  $ENV{TZ} = ($original_time_zone or '');
}

## handle the help command
## ARG  1: the help option
## ARG  2: the default option
## RETURN: None
sub help_command {
  my $option = $_[0];
  my $default = $_[1];

  # set the page mode
  my $prev_pause = $configs{pause};
  $configs{pause} = 'page';

  if(!$option or $option =~ /^command/i) { # commands help
    command_help();
  }
  elsif($option =~ /^about/i) { # about help
    about_help();
  }
  elsif($option =~ /^doc/i) { # documentation help
    doc_help();
  }
  elsif($option =~ /^init/i) { # initialization help
    init_help();
  }
  elsif($option =~ /^script/i) { # script help
    script_help();
  }
  elsif($option =~ /^select/i) { # select help
    select_help();
  }
  elsif($option =~ /^insert/i) { # insert help
    insert_help();
  }
  elsif($option =~ /^update/i) { # update help
    update_help();
  }
  elsif($option =~ /^delete/i) { # delete help
    delete_help();
  }
  elsif($option =~ /^multiple/i) { # multiple help
    multiple_help();
  }
  elsif($option =~ /^display/i) { # display help
    display_help();
  }
  elsif($option =~ /^history/i) { # history help
    history_help();
  }
  elsif($option =~ /^setting/i) { # setting help
    setting_help();
  }
  elsif($option =~ /^color/i) { # color help
    color_help();
  }
  elsif($option =~ /^variable/i) { # variable help
    variable_help();
  }
  elsif($option =~ /^completion/i) { # name completion help
    completion_help();
  }
  elsif($option =~ /^alias/i) { # alias commands help
    alias_help();
  }
  elsif($option =~ /^spool/i) { # spool help
    spool_help();
  }
  elsif($option =~ /^feature/i) { # feature help
    feature_help();
  }
  else { # help help
    help_help();
  }
  output("\n");

  $configs{pause} = $prev_pause;
  exit 1 if($default and $default eq 'usage');
}

## print the help message
## ARG   : None
## RETURN: None
sub help_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Help:'.RESET."\n\n  ".
  E('help').' ['.E('command')."]   -- list all of valid commands\n  ".
  E('help about')."       -- list the tool information\n  ".
  E('help doc')."         -- list all of documentation\n  ".
  E('help init')."        -- explain how to set up the initialization file\n  ".
  E('help script')."      -- explain how to make a script file\n  ".
  E('help select')."      -- explain what are the valid select statements\n  ".
  E('help insert')."      -- explain what are the valid insert statements\n  ".
  E('help update')."      -- explain what are the valid update statements\n  ".
  E('help delete')."      -- explain what are the valid delete statements\n  ".
  E('help multiple')."    -- explain how to make a multiple command\n  ".
  E('help display')."     -- explain how the display works\n  ".
  E('help history')."     -- explain how the history works\n  ".
  E('help setting')."     -- explain how the setting works\n  ".
  E('help color')."       -- describe what are the valid colors\n  ".
  E('help variable')."    -- explain how to use variables\n  ".
  E('help completion')."  -- explain how the name completion works\n  ".
  E('help alias')."       -- explain how to set up the alias commands\n  ".
  E('help spool')."       -- explain how to spool the results\n  ".
  E('help feature')."     -- show the list of features this tool has\n  ".
  E('help help')."        -- display this help message\n");
}

## list some information about this tool
## ARG   : None
## RETURN: None
sub about_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool About Help:'.RESET."\n
  This tool was developed by Rui Chen for providing the user a better SQL environment. It was first
  released on December 2002.

  Thanks for using this tool. For bugs, questions and features, please email to ruichen\@gmail.com.
  ");
}

## print the all of documentation message
## ARG   : None
## RETURN: None
sub doc_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Documentation Help:'.RESET."\n
  The documentation contains all of the help information of the dbtool.\n\n");

  help_help();
  command_help();
  about_help();
  init_help();
  script_help();
  select_help();
  insert_help();
  update_help();
  delete_help();
  multiple_help();
  display_help();
  history_help();
  setting_help();
  color_help();
  variable_help();
  completion_help();
  alias_help();
  spool_help();
  feature_help();
}

## print the initialization help message
## ARG   : None
## RETURN: None
sub init_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Initialization Help:'.RESET."\n
  The initialization file is parsed into the dbtool when starting. It is located under the same
  directory of this tool with the name \".dbtool_init\" in default. A user could specify a different
  initialization file by using the switch \"-init=<init file>\" in the command line.

  The initialization file can be used to set any valid commands to initialize the dbtool
  environment. Specially, the \"alias commands\" can be created in this file. The format of
  \"alias commands\" can be found by executing the command \"help alias\".

  Example of the .dbtool_init file:
    # some initialization
    set display landscape
    color sys_date red

    # some alias commands
    alias sysdate  select to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') sys_date from dual;

    alias order    select * from orders where order_id = '\$1' and rownum <= \$2;
    alias order2   select * from orders where order_id = '&order_id';
    alias customer select * from customers where cust_id = \$1;
    alias string   select * from strings where string like '%\$1%';
  ");
}

## print the script help message
## ARG   : None
## RETURN: None
sub script_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Script Help:'.RESET."\n
  Script file can be used to execute a batch of dbtool commands. The text after \"#\" or \"--\" is
  ignored as comments.

  For example, the following commands can be put into a script file:
    # some comments here
    set brackets off
    set rownum on
    set display portrait
    select * from strings where rownum=1; -- comments at the end of the line
    set display landscape
    select
    *
    from strings where rownum<=2;
  ");
}

## print the select help message
## ARG   : None
## RETURN: None
sub select_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Select Help:'.RESET."\n
  This tool accepts any valid select statements. The select command can be inputted in multiple
  lines until a \";\" is encountered.

  The enhanced select command in DBTool is that it could have a \"DBTOOL_OUTPUT\" appended after
  the select command to control the query output by running a series of pipe shell commands.

  The # and -- should be escaped if they are not comments.
  ");
}

## print the insert help message
## ARG   : None
## RETURN: None
sub insert_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Insert Help:'.RESET."\n
  This tool accepts any valid insert statements. The insert command can be inputted in multiple
  lines until a \";\" is encountered.

  The # and -- should be escaped if they are not comments.

  You can execute \"commit\" or \"rollback\" for the insertions afterwards.
  ");
}

## print the update help message
## ARG   : None
## RETURN: None
sub update_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Update Help:'.RESET."\n
  This tool accepts any valid update statements. The update command can be inputted in multiple
  lines until a \";\" is encountered.

  The # and -- should be escaped if they are not comments.

  You can execute \"commit\" or \"rollback\" for the updates afterwards.
  ");
}

## print the delete help message
## ARG   : None
## RETURN: None
sub delete_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Delete Help:'.RESET."\n
  This tool accepts any valid delete statements. The delete command can be inputted in multiple
  lines until a \";\" is encountered.

  The # and -- should be escaped if they are not comments.

  You can execute \"commit\" or \"rollback\" for the deletions afterwards.
  ");
}

## print the multiple command help message
## ARG   : None
## RETURN: None
sub multiple_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Multiple Command Help:'.RESET."\n
  Each command line can have multiple commands separated by \";\". Therefore, any command with \";\"
  inside must be escaped by \"\\\".

  For example, the following is a valid multiple command:
    db dbname1; /; db dbname2; /; db dbname3; /

  or this one:
    !date; db dbname1; select count(*) from table1;
  ");
}

## print the display help message
## ARG   : None
## RETURN: None
sub display_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Display Help:'.RESET."\n
  There are many display modes in this tool: \"landscape\", \"portrait\", \"row\", \"csv\", \"xml\"
  and \"compare\". The setting \"display\" has the current display mode.

  \"landscape\" mode displays the data fields line by line for each record. Each line shows the
  field name and field value pair.

  \"portrait\" mode displays each record in one or more lines. The heading of the record is shown
  before the records.

  \"row\" mode displays each record in exactly one line. The heading of the record is shown before
  the records in one or two lines. The setting \"linesize\" and \"columnsize\" can effectively
  affect the display result.

  \"csv\" mode displays the query result in csv format.

  \"xml\" mode displays the query result in xml format.

  \"compare\" mode displays the records side by side. The setting \"linesize\" and \"columnsize\"
  can effectively affect the display result.

  User can hit \"Ctrl+C\" to cancel the executing of a command.
  ");
}

## print the history help message
## ARG   : None
## RETURN: None
sub history_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool History Help:'.RESET."\n
  The executed commands are automatically saved into the history, which will be persisted in the
  history file. The history file is under $vars{TEMP_PATH} directory with the name \".dbtool_hist\"
  postfixed by the dot and login name in default. A user can specify his own history file when
  starting the dbtool by using the switch \"-hist=<history file>\".

  The setting \"savehistory\" is used to control how many history commands can be saved.

  Only multiple commands and DML commands are saved to the history file.
  ");
}

## print the setting help message
## ARG   : None
## RETURN: None
sub setting_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Setting Help:'.RESET."\n
  Some of the settings during the execution are automatically saved into the setting file. The
  setting file is under $vars{TEMP_PATH} directory with the name \".dbtool_setting\" postfixed
  by the dot and login name in default. A user can specify its own setting file when starting
  the dbtool by using the switch \"-setting=<setting file>\".

  Deleting the setting file can be used to remove the user specific settings.

  The setting \"database\" is used to enter a different database. You can have the following
  set database commands:
    set database db_user\@db_name           # Enter a Oracle database with a user
    set database db_user/password\@db_name  # Enter a Oracle database with a user
    set database db_user\@db_name mysql     # Enter a MySQL database with a specified user

  The setting \"user\" is used to change the database user in the current database.

  The setting \"display\" have many modes: landscape, portrait, row, csv, xml and compare. See
  \"help display\".

  The setting \"linesize\" decides the maximum length of each line in the portrait and row display
  mode. 0 value means using the current window line size. The default linesize is the window width.

  The setting \"columnsize\" decides the maximum length of each column in the row display mode. 0
  value means maximum column size is the line size. The default columnsize is $configs{columnsize}.

  The setting \"pause\" is used to pause the display, and continue after any key is pressed.

  The setting \"rownum\" is used to control if displaying the row number for each result record.

  The setting \"blankrow\" is used to control if displaying a blank line between the records.

  The setting \"brackets\" is used to control if displaying the field value with brackets.

  The setting \"nondisplayable\" is used to turn on/off displaying the nondisplayable characters.

  The setting \"redisplay\" is used to turn on/off redisplaying the history command. \"multioff\"
  option is to turn off redisplaying the multiple command.

  The setting \"spoolformat\" is used to set the default spool output format.

  The setting \"sort\" is used to sort the fields by column, value or default mode. Note sort by
  value is not applicable to the \"compare\" display mode.

  The setting \"rowsperheading\" decides how many records are following after the heading. 0 value
  means only one heading at first then following with all of the records.

  The setting \"heading\" is used to control if displaying the heading.

  The setting \"savehistory\" is to set the number of history commands to be saved.

  The setting \"quickshell\" is to set the shell commands which can be used directly in the tool.
  ");
#  The setting \"namecompletion\" is to set if using the name completion of the commands.
}

## print the color help message
## ARG   : None
## RETURN: None
sub color_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Color Help:'.RESET."\n
  The select results can be displayed in different colors for different fields by using the command
  \"color\". The defined column colors can be removed by specifying the color to be \"delete\".

  This command can be used to set the system message colors too, such as: :EMPHASIS, :HEADING,
  :HISTORY, :MESSAGE, :NOTE.

  Colors will support the Term::ANSIColor, such as: CLEAR, RESET, BOLD, UNDERLINE, UNDERSCORE,
  BLINK, REVERSE, CONCEALED, BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE, ON_BLACK,
  ON_RED, ON_GREEN, ON_YELLOW, ON_BLUE, ON_MAGENTA, ON_CYAN, ON_WHITE.
  ");
}

## print the variable help message
## ARG   : None
## RETURN: None
sub variable_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Variable Help:'.RESET."\n
  The commands can have variables which can be declared by \"&\" plus an identifier. When such
  command is executed, the system will ask the user to give the values of those variables.
  ");
}

## print the name completion help message
## ARG   : None
## RETURN: None
sub completion_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Name Completion Help:'.RESET."\n
  When input a command, the file names or table names or column names can be completed automatically
  if the setting \"namecompletion\" is enabled.

  The name completion for tables and columns only works for lower case.
  ");
}

## print the alias command help message
## ARG   : None
## RETURN: None
sub alias_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Alias Command Help:'.RESET."\n
  Users can create the alias commands in the initialization file or by executing alias command.

  The format of the alias commands would be like:
    alias ALIAS_COMMAND_NAME ACTUAL_COMMAND

  The parameters in the ACTUAL_COMMAND are noted by \$number, i.e., \$1 is the first parameter, \$2
  is the second parameter, etc.

  For example:
    alias sysdate  select to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') sys_date from dual;

    alias order    select * from orders where order_id = '\$1' and rownum <= \$2;
    alias order2   select * from orders where order_id = '&order_id';
    alias customer select * from customers where cust_id = \$1;
    alias string   select * from strings where string like '%\$1%';

  Then in the tool, you can run such commands:
    ALIAS_COMMAND_NAME param1 param2 ... paramN

  The parameters are separated by space. Each quoted ' or \" string is regarded as one parameter.
  Inside the parameter, the quote ' or \" and escape \\ need to be escaped by \"\\\". In SQL, the
  \'\' is regarded as the quote '. So in the parameter, it can be written as \"\\'\\'\".

  Such as:
    sysdate
    order 123-456789 2
    customer 123456789
    string \"Welcome to\"      # find the strings containing the string \"Welcome to\"
    string 'Can\\'\\'t'        # find the strings containing the string \"Can't\"
  ");
}

## print the spool help message
## ARG   : None
## RETURN: None
sub spool_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Spool Help:'.RESET."\n
  The commands and their results can be saved to the spool file by executing the command \"spool\"
  plus a valid output file. Stop spooling after executing the command \"spool off\".

  The spool output has three formats: \"unix\", \"dos\" and \"html\". The default format is set by
  the command \"set spoolformat\".
  ");
}

## print the feature help message
## ARG   : None
## RETURN: None
sub feature_help {
  output("\n".$colors{'!:MESSAGE'}.'DBTool Feature Help:'.RESET."\n
  - Supporting Oracle and MySQL databases;
  - Support DML statements,
  - Not requiring SQL user/password to login,
  - Navigating different databases easily,
  - Query result associating with the column names,
  - Being able to output the query result in different colors for different fields,
  - Easily obtaining indexes, constraints, sessions, triggers, sources, progress and explain plans,
  - Easily setting up user own shortcut commands,
  - Persisting the history DML commands,
  - Accessing history commands easily,
  - Supporting spool with unix, dos and html output format,
  - Easily accessing the shell command,
  - Being able to execute script files,
  - One input line being able to contain multiple commands,
  - Being able to display the results in landscape, portrait, row, csv, xml and compare modes,
  - Being able to pause the output,
  - Being able to sort record fields,
  - Being able to choose records out of a query;
  - Being able to change output colors,
  - Being able to hide selected columns,
  - Remembering your current settings.
  ");
#  - Table and column name auto completion,
}

## print the command help message
## ARG   : None
## RETURN: None
sub command_help {
  output("\n$colors{'!:MESSAGE'}All Commands:".RESET."\n\n".
  '  '.E('help').S(39).
           "-- display all of the commands\n".
  '  '.E('help help').S(34).
           "-- display all of help commands\n\n");

  if($vars{cgi_mode}) {
    output('  '.E('set database').' <db_name>'.S(21).
           "-- set the database\n");
  }
  else {
    output('  '.E('set database').' <[user'.E('/').'pass'.E('@').']dbname> ['.E('mysql').']'.S(2).
           "-- set the database\n".
    '  '.E('set display').' <'.E('l').'['.E('andscape').']|'.E('p').'['.E('ortrait')."]|\n".
    S(15).E('r').'['.E('ow').']|['.E('cs').']'.E('v').'|'.E('x').'['.E('ml').']|'.E('c').'['.E('ompare').']>'.S(2).
           "-- set the display method\n".
    '  '.E('set user').' <dbuser>'.S(26).
           "-- set the database user name\n".
    '  '.E('set pause').' <'.E('nostop').'|'.E('record').'|number|'.E('page').'>'.S(6).
           "-- set the output pause\n".
    '  '.E('set spoolformat').' <'.E('unix').'|'.E('dos').'|'.E('html').'>'.S(12).
           "-- set the default spool output format\n".
    '  '.E('set quickshell').' <shell command|...>'.S(9).
           "-- set the quick shell commands\n");

#    if(!$vars{mysql_connect}) {
#      output('  '.E('set namecompletion').' <'.E('off').'|'.E('on').'>'.S(16).
#           "-- set if name completes\n");
#    }
  }

  if(!$vars{cgi_mode}) {
    output('  '.E('set linesize').'|'.E('columnsize').' <size>'.S(13).
             "-- set the line or column size\n".
    '  '.E('set rownum').'|'.E('blankrow').'|'.E('brackets').' <'.E('off').'|'.E('on').'>'.S(6).
             "-- set if display rownum, blank row, brackets\n");
  }
 
#  if(!$vars{mysql_connect}) {
#    output('  '.E('set nondisplayable').' <'.E('on').'|'.E('off').'>'.S(16).
#           "-- set if display nondisplayable characters\n");
#  }

  if(!$vars{cgi_mode}) {
    output('  '.E('set redisplay').' <'.E('multioff').'|'.E('on').'|'.E('off').'>'.S(12).
             "-- set if redisplay the history command\n".
    '  '.E('set sort').' <'.E('default').'|'.E('column').'|'.E('value').'> ['.E('asc').'|'.E('desc').
             "] -- set the sorting of the display\n".
    '  '.E('set heading').' <'.E('on').'|'.E('off').'>'.S(23).
             "-- set if display the heading\n".
    '  '.E('set rowsperheading').' <number>'.S(16).
             "-- set the number of rows following each heading\n".
    '  '.E('set savehistory').' <number>'.S(19).
             "-- set the number of histories to be saved\n\n");
  }

  if($vars{mysql_connect}) {
    output('  '.E('use').' <dbname>'.S(31).
           "-- change to another MySQL database\n".
    '  '.E('show databases').'|'.E('tables').S(22).
           "-- show available databases or tables\n\n");
  }

  output('  '.E('show all').'|'.E('setting').'|'.E('hiding').'|'.E('alias').'|'.E('color').S(8).
           "-- show all configs,settings,hidings,aliases,colors\n");

  if(!$vars{cgi_mode}) {
    output('  '.E('color').' <column name> <ANSIColor|'.E('delete').
           ">     -- set or delete the color of the column\n".
    '  '.E('hide').' <column name> <'.E('on').'|'.E('off').'|'.E('delete').'>'.S(9).
           "-- set if hide the column\n".
    '  '.E('hideall').' <'.E('on').'|'.E('off').'|'.E('delete').'>'.S(20).
           "-- set if hide all of the hidden columns\n");
  }

  output('  '.E('alias').' <alias name> <command|'.E('delete').'>'.S(8).
         "-- set or delete the alias command\n".
  '  '.E('prompt').' <string>'.S(28).
           "-- output the string\n".
  '  '.E('localtime').' [zone]'.S(27).
           "-- output the different local times\n\n");

  if(!$vars{cgi_mode}) {
    output('  '.E('spool').' '.E('off').'|<filename ['.E('unix').'|'.E('dos').'|'.E('html').']>'.
           S(7)."-- stop or start outputting to the file\n".
    '  '.E('exit').'|'.E('quit').S(34).
           "-- exit the tool\n\n".

    '  '.E('!').'<shell command>'.S(27).
           "-- execute the shell command\n");
  }

  if(!$vars{cgi_mode}) {
    output('  '.E('hist').'['.E('ory').']'.S(34).
    "-- list the history commands\n".
    '  '.E('/').'|<history idx>'.S(28).
    "-- execute the last/specified history command\n".
    '  '.E('choose').' <history idx> [<rowX, rowY, ...>]   '.
    "-- choose the selected rows of the history command\n".
    '  '.E('@').'|'.E('run').' <sql file>'.S(27).
    "-- run the sql script file\n\n");
  }

  output('  '.E('desc').' <table name>'.S(26).
         "-- list the table schema\n".
         '  '.E('index').' <table name>'.S(25).
         "-- list the indexes of the table\n");

  if(!$vars{mysql_connect}) {
    output('  '.E('constraint').' <table name>'.S(20).
           "-- list the constraints of the table\n".
    '  '.E('trigger').' <table name>'.S(23).
           "-- list the triggers of the table\n".
    '  '.E('snapshot').' <table name>'.S(22).
           "-- list the snapshot information of this table\n".
    '  '.E('view').' <table name>'.S(26).
           "-- list the view information of this view\n".
    '  '.E('synonym').' <synonym name>'.S(21).
           "-- list the synonym information of this synonym\n".
    '  '.E('sequence').' <sequence name>'.S(19).
           "-- list the sequence information of this sequence\n".
    '  '.E('type').' <table name>'.S(26).
           "-- list the type information of this table\n".
    '  '.E('source').' <source name>'.S(23).
           "-- list the function, procedure or package\n".
    '  '.E('session').' [osuser]'.S(27).
           "-- list the SQL sessions\n".
    '  '.E('progress').' [<rows per commit>] [osuser]'.S(6).
           "-- list the DML progress\n\n");
  }
  else {
    output("\n");
  }

  if(!$vars{mysql_connect} and !$vars{cgi_mode}) {
    output('  '.E('explain').' '.E('/').'|<number>'.S(25).
           "-- explain plan for the last/specified history command\n");
  }

  if(!$vars{mysql_connect}) {
    if(!$vars{cgi_mode}) {
      output('  '.E('explain').' <select|insert|update|delete stmt>'.
           " -- explain plan for the dml command\n\n");
    }
    else {
      output('  '.E('explain').' <select stmt>'.S(22).
           "-- explain plan for the select command\n\n");
    }
  }

  output('  '.E('select').' <'.E('*')."\|<col1, ..., colN>>\n".
  '  '.E('from').' <table1, ..., tableM> [condition]'.
  ($vars{cgi_mode}? E(';') : "\n".'  ['.E('DBTOOL_OUTPUT').' <|shell command>]'.E(';').S(6)).
           "    -- select records from tables\n\n");

  if(!$vars{cgi_mode} and !$vars{dml_disabled}) {
    output('  '.E('insert into')." <table name>\n".
    '  '.E('values (').'<val1, ..., valN>'.E(');').S(16).
           "-- insert a record to the table\n\n".

    '  '.E('update').' <table name> '.E('set')."\n".
    '  <col1=val1, ..., colN=valN> [condition]'.E(';').
           "   -- update records of the table\n\n".

    '  '.E('delete from').' <table name> [condition]'.E(';').
           "      -- delete records from the table\n\n".

    '  '.E('commit').'|'.E('rollback').S(28).
           "-- commit or rollback the changes\n");
  }

  if(!$vars{cgi_mode} and !$vars{ddl_disabled}) {
    output('  '.E('create').'|'.E('alter').'|'.E('grant').'|'.E('drop').'|'.E('analyze')." <stmt>".S(5).
           "-- execute the DDL command\n\n");
  }

#  if(%aliases) {
#    output("\n$colors{'!:MESSAGE'}Alias Commands:".RESET."\n\n");
#
#    my $max_key_len = 0;
#    foreach my $key (keys %aliases) {
#      $max_key_len = max($max_key_len, length($key));
#    }
#    foreach my $key (sort(keys %aliases)) {
#      output('  '.E("$key").S($max_key_len-length($key))." : $aliases{$key}\n");
#    }
#  }
}

## emphasize the text
## ARG   : the text
## RETURN: the emphasized text
sub E {
  return $colors{'!:EMPHASIS'}.$_[0].RESET;
}

## padding to space
## ARG  1: the number of padding times
## RETURN: the result string
sub S {
  return pad($_[0], ' ');
}

# ================================================================================

sub new_session {
  my($sid) = shift;
  my($user) = shift || '';
  my($password) = shift || '';
  my($new_mysql_connect) = shift || '';

  my($self) = {};
  my($dbh, $ccs);

  $ccs = join('|', $sid, $user, $password);

  $dbh = get_dbh($ccs, $sid, $user, $password, $new_mysql_connect);
  if(! defined ($dbh)) {
    error('Unable to connect to the database: '.E($sid).' (with the db user '.E($user).").".
          "\nTip: please go to hyper7: \n".
          "run /x/home/xin/bin/update_dbtool_tnsnames to update the tnsnames of the target stage.\n".
          "If the target stage is PPSB: please run /x/home/xin/bin/update_dbtool_tnsnames-PPSB to update.\n".
          "Thanks!");
    return undef;
  }

  $self->{cs} = $ccs;
  $self->{dbh} = $dbh;
  $self->{report_errors} = 1;
  $self->{warn_about_duplicate_hash_keys} = 1;
  $self->{sid} = $sid;
  if(!$new_mysql_connect) {
    $self->{dbh}->{RaiseError} = 1;
    $self->{dbh}->{AutoCommit} = 0;
  }

  return bless $self, $sid;
}

sub get_dbh {
  my($cs, $sid, $user, $password, $new_mysql_connect) = @_;
  my($driver_handle, $dbh);

  if(defined $vars{ci_to_dbh}{$cs}) {
    $vars{crc}{$vars{ci_to_dbh}{$cs}}++;
    return $vars{ci_to_dbh}{$cs};
  }

  eval {
    if($new_mysql_connect) {
      $dbh = DBI->connect("dbi:mysql:$sid", $user, $password);
    }
    else {
#      $dbh = DBI->connect("DBI:Oracle:$sid",$user,$password, {RaiseError=>1,AutoCommit=>0});
#      $dbh = DBI->connect("DBI:Oracle:$sid",$user,$password, {RaiseError=>1,AutoCommit=>0,LongTruncOk=>1});
      $dbh = DBI->connect("DBI:Oracle:$sid",$user,$password, {RaiseError=>1,AutoCommit=>0,LongReadLen=>1000000,LongTruncOk=>1});
    }
  };
  return undef if(check_session_error() || !$dbh);

  $vars{ci_to_dbh}{$cs} = $dbh;
  $vars{crc}{$dbh}++;

  return $dbh;
}

sub cursor_register {
  my($cursor_ref) = shift;
  $session->{cursors} = $cursor_ref;
  %{$session->{read_only_cursors}} = %$cursor_ref;
}

sub cursor_open {
  my($cursor_name) = shift @_;
  my($cache_cursor) = shift @_;
  my($sql, $cursor);

  if(!defined ($sql = $session->{cursors}{$cursor_name})) {
    error("cursor name \"$cursor_name\" unknown");
    return undef;
  }

  if(!defined($cursor = $session->{cursor_cache}{$cursor_name})) {
    eval {
      if(!defined($cursor = $session->{dbh}->prepare($sql))
          and $session->{report_errors}) {
        error($session->{dbh}->errstr);
        return undef;
      }
    };

    if($cache_cursor) {
      if(scalar keys % { $session->{cursor_cache}}
            < $vars{max_cursors}) {
        $session->{cursor_cache}{$cursor_name} = $cursor;
      }
    }
  }

  return($cursor);
}

sub cursor_do {
  my($cursor_name) = shift @_;
  my($return_value, $sql);

  if(!defined ($sql = $session->{cursors}{$cursor_name})) {
    error("cursor name \"$cursor_name\" unknown");
    return undef;
  }
  $return_value = $session->{dbh}->do($sql, undef, @_);
  if(!defined $return_value and $session->{report_errors}) {
    error($session->{dbh}->errstr());
  }
  return $return_value;
}

sub aa_for_cursor {
  my($cursor_name) = shift @_;
  my($cache_cursor) = shift @_;

  my($cursor, $result_row, @results);
  if(!defined($cursor = cursor_open($cursor_name, $cache_cursor))) {
    return undef;
  }

  my $return_value = $cursor->execute(@_);
  if(!defined $return_value and $session->{report_errors}) {
    error($session->{dbh}->errstr());
  }
  else {
    my ($array_ref);
    while ($array_ref = $cursor->fetch) {
      undef $result_row;

      $result_row = [ @$array_ref ];
      push(@results, $result_row);
    }
  }

  $cursor->finish() if !$cache_cursor;
  return \@results;
}

sub DESTROY_SESSION {
  my($cursor);

  foreach $cursor (values %{$session->{cursor_cache}}) {
    $cursor->finish() if $cursor;
  }

  foreach $cursor (keys %{$session->{cursors}}) {
    if($session->{cursors}->{$cursor} ne
      $session->{read_only_cursors}->{$cursor}) {

      warn "WARNING: OracleSession cursor $cursor modified on exit.\n";
    }
  }

  $vars{crc}{$session->{dbh}}--;

  if($vars{crc}{$session->{dbh}} == 0) {
    $session->{dbh}->disconnect() if defined $session->{dbh};
    delete $vars{ci_to_dbh}{$session->{cs}};
  }
}

sub check_error {
  return ($DBI::err, $DBI::errstr, $DBI::state);
}

# ==============================================================================

1;

__END__

=head1 USAGE

    ######################################################
    ####        DBTool 6.0 (SQL DATABASE TOOL)        ####
    ####                                              ####
    ####               Author: Rui Chen               ####
    ####          Contact: ruichen@gmail.com          ####
    ####                                              ####
    ####             All rights reserved.             ####
    ######################################################

    Just execute the tool without any parameters.

=head1 SWITCHES

    The following switches in the command can be specified: 

    -init=initfile    # set the initialization file
    -hist=histfile    # set the history file
    -setting=setfile  # set the setting file
    -help             # print the help messages

=head1 INFO

    A tool to execute SQL commands with a friendly interface.
