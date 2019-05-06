/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 Norsk Rikskringkasting AS. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

require('./scs-helpers.js');

class Cooloff {
  constructor(name, cooloffLimit) {
    this.name = name;
    this.cooloffLimit = cooloffLimit;
    this.count = 0;
    this.ready = true;
    debug("Cooloff constructed");
  }
  
  tick() {
    this.count--;
    if (this.count <= 0) {
      this.count = 0;
      this.ready = true;
    }
    debug("Cooloff(" + this.name + ").tick(), count: " + this.count +
          ", ready: " + this.ready);
  }
  
  fire() {
    if (this.ready) {
      this.ready = false;
      this.count = this.cooloffLimit;
      debug("Cooloff(" + this.name + ").fire(): True, count: " +
            this.count + ", ready: " + this.ready);
      return true;
    }
    
    debug("Cooloff(" + this.name + ").fire(): False, count: " +
          this.count + ", ready: " + this.ready);
    return false;
  }
}

module.exports = Cooloff;
