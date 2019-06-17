/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::u32::MAX;

use crate::constants::NodeId;

/// Maintains a collection of N boolean flags that can efficiently be reset by incrementing a
/// single integer
pub struct ValidFlags {
    valid_flags: Vec<u32>,
    valid_flag: u32,
}

impl ValidFlags {
    pub fn new(num_nodes: usize) -> Self {
        ValidFlags {
            valid_flags: vec![0; num_nodes],
            valid_flag: 1,
        }
    }

    pub fn is_valid(&self, node: NodeId) -> bool {
        self.valid_flags[node] == self.valid_flag
    }

    pub fn set_valid(&mut self, node: NodeId) {
        self.valid_flags[node] = self.valid_flag;
    }

    pub fn invalidate_all(&mut self) {
        if self.valid_flag == MAX {
            self.valid_flags = vec![0; self.valid_flags.len()];
            self.valid_flag = 1;
        } else {
            self.valid_flag += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::valid_flags::ValidFlags;

    #[test]
    fn set_valid_and_invalidate() {
        let mut flags = ValidFlags::new(5);
        assert!(!flags.is_valid(3));
        flags.set_valid(3);
        assert!(flags.is_valid(3));
        flags.invalidate_all();
        assert!(!flags.is_valid(3));
    }
}
