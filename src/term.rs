use crate::cmd::cmd_const::SUPPORT_CMD_LIST;
use crate::cmd::cmd_runner::CmdRunner;
use crate::cmd::cmd_utils::{all_support_cmd_string, default_log_handler};
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Completion, History, Input};
use std::collections::VecDeque;
use std::process;

const MAX_INPUT_HISTORY: usize = 1000;
const PROMPT_STR: &str = "monograph_waiter>";

pub struct CmdCli;

impl CmdCli {
    pub async fn start(&self) {
        let mut history = InputHistory::default();
        let completion = InputCompletion::default();
        let logger = default_log_handler().unwrap();
        let runner = CmdRunner::new(&logger);
        loop {
            if let Ok(cmd) = Input::<String>::with_theme(&ColorfulTheme::default())
                .with_prompt(PROMPT_STR.to_string())
                .history_with(&mut history)
                .completion_with(&completion)
                .interact_text()
            {
                if cmd == "help" {
                    println!("all command list \n{}", all_support_cmd_string());
                    continue;
                }
                if cmd == "exit" || cmd == "quit" {
                    process::exit(0);
                }
                if !SUPPORT_CMD_LIST.contains(&cmd.as_str()) {
                    println!(
                        "Warn: not support command {}.For now support command list: \n{}",
                        cmd,
                        all_support_cmd_string()
                    )
                } else {
                    let cmd_status = runner.run(cmd.to_string()).await;
                    println!();
                    println!("{:#?}", cmd_status);
                }
            }
        }
    }
}

struct InputCompletion {
    all_cmd_list: Vec<String>,
}

impl Completion for InputCompletion {
    fn get(&self, input: &str) -> Option<String> {
        let matches = self
            .all_cmd_list
            .iter()
            .filter(|option| option.starts_with(input))
            .collect::<Vec<_>>();

        if matches.len() == 1 {
            Some(matches[0].to_string())
        } else {
            None
        }
    }
}

impl Default for InputCompletion {
    fn default() -> Self {
        Self {
            all_cmd_list: SUPPORT_CMD_LIST.iter().map(|cmd| cmd.to_string()).collect(),
        }
    }
}

struct InputHistory {
    max: usize,
    history: VecDeque<String>,
}

impl Default for InputHistory {
    fn default() -> Self {
        InputHistory {
            max: MAX_INPUT_HISTORY,
            history: VecDeque::default(),
        }
    }
}

impl<T: ToString> History<T> for InputHistory {
    fn read(&self, pos: usize) -> Option<String> {
        self.history.get(pos).cloned()
    }

    fn write(&mut self, input_val: &T) {
        if SUPPORT_CMD_LIST.contains(&input_val.to_string().as_str()) {
            if self.history.len() == self.max {
                self.history.pop_back();
            }
            self.history.push_front(input_val.to_string());
        }
    }
}
