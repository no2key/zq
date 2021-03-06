// from https://github.com/fitzgen/glob-to-regexp

function Reglob(glob, opts) {
  if (typeof glob !== 'string') {
    throw new TypeError('Expected a string');
  }

  var str = String(glob);

  // The regexp we are building, as a string.
  var reStr = "";

  // Whether we are matching so called "extended" globs (like bash) and should
  // support single character matching, matching ranges of characters, group
  // matching, etc.
  var extended = opts ? !!opts.extended : false;

  // When globstar is _false_ (default), '/foo/*' is translated a regexp like
  // '^\/foo\/.*$' which will match any string beginning with '/foo/'
  // When globstar is _true_, '/foo/*' is translated to regexp like
  // '^\/foo\/[^/]*$' which will match any string beginning with '/foo/' BUT
  // which does not have a '/' to the right of it.
  // E.g. with '/foo/*' these will match: '/foo/bar', '/foo/bar.txt' but
  // these will not '/foo/bar/baz', '/foo/bar/baz.txt'
  // Lastely, when globstar is _true_, '/foo/**' is equivelant to '/foo/*' when
  // globstar is _false_
  var globstar = opts ? !!opts.globstar : false;

  // If we are doing extended matching, this boolean is true when we are inside
  // a group (eg {*.html,*.js}), and false otherwise.
  var inGroup = false;

  // RegExp flags (eg "i" ) to pass in to RegExp constructor.
  var flags = opts && typeof( opts.flags ) === "string" ? opts.flags : "";

  var c;
  for (var i = 0, len = str.length; i < len; i++) {
    c = str[i];

    switch (c) {
    case "/":
    case "$":
    case "^":
    case "+":
    case ".":
    case "(":
    case ")":
    case "=":
    case "!":
    case "|":
      reStr += "\\" + c;
      break;

    case "?":
      if (extended) {
        reStr += ".";
        break;
      }

    case "[":
    case "]":
      if (extended) {
        reStr += c;
        break;
      }

    case "{":
      if (extended) {
        inGroup = true;
        reStr += "(";
        break;
      }

    case "}":
      if (extended) {
        inGroup = false;
        reStr += ")";
        break;
      }

    case ",":
      if (inGroup) {
        reStr += "|";
        break;
      }
      reStr += "\\" + c;
      break;

    case '\\':
      if (str[i+1] == '*') {
        i++;
        reStr += '\\*';
      } else {
        reStr += c;
      }
      break;

    case "*":
      // Move over all consecutive "*"'s.
      // Also store the previous and next characters
      var prevChar = str[i - 1];
      var starCount = 1;
      while(str[i + 1] === "*") {
        starCount++;
        i++;
      }
      var nextChar = str[i + 1];

      if (!globstar) {
        // globstar is disabled, so treat any number of "*" as one
        reStr += ".*";
      } else {
        // globstar is enabled, so determine if this is a globstar segment
        var isGlobstar = starCount > 1                      // multiple "*"'s
          && (prevChar === "/" || prevChar === undefined)   // from the start of the segment
          && (nextChar === "/" || nextChar === undefined);   // to the end of the segment

        if (isGlobstar) {
          // it's a globstar, so match zero or more path segments
          reStr += "((?:[^/]*(?:\/|$))*)";
          i++; // move over the "/"
        } else {
          // it's not a globstar, so only match one path segment
          reStr += "([^/]*)";
        }
      }
      break;

    default:
      reStr += c;
    }
  }

  // When regexp 'g' flag is specified don't
  // constrain the regular expression with ^ & $
  if (!flags || !~flags.indexOf('g')) {
    reStr = "^" + reStr + "$";
  }

  return reStr;
}


function IsGlobby(s) {
  return (s.indexOf("*") >= 0 || s.indexOf("?") >= 0);
}

var reglob = {
  Reglob,
  IsGlobby,
};

function peg$subclass(child, parent) {
  function ctor() { this.constructor = child; }
  ctor.prototype = parent.prototype;
  child.prototype = new ctor();
}

function peg$SyntaxError(message, expected, found, location) {
  this.message  = message;
  this.expected = expected;
  this.found    = found;
  this.location = location;
  this.name     = "SyntaxError";

  if (typeof Error.captureStackTrace === "function") {
    Error.captureStackTrace(this, peg$SyntaxError);
  }
}

peg$subclass(peg$SyntaxError, Error);

peg$SyntaxError.buildMessage = function(expected, found) {
  var DESCRIBE_EXPECTATION_FNS = {
        literal: function(expectation) {
          return "\"" + literalEscape(expectation.text) + "\"";
        },

        "class": function(expectation) {
          var escapedParts = "",
              i;

          for (i = 0; i < expectation.parts.length; i++) {
            escapedParts += expectation.parts[i] instanceof Array
              ? classEscape(expectation.parts[i][0]) + "-" + classEscape(expectation.parts[i][1])
              : classEscape(expectation.parts[i]);
          }

          return "[" + (expectation.inverted ? "^" : "") + escapedParts + "]";
        },

        any: function(expectation) {
          return "any character";
        },

        end: function(expectation) {
          return "end of input";
        },

        other: function(expectation) {
          return expectation.description;
        }
      };

  function hex(ch) {
    return ch.charCodeAt(0).toString(16).toUpperCase();
  }

  function literalEscape(s) {
    return s
      .replace(/\\/g, '\\\\')
      .replace(/"/g,  '\\"')
      .replace(/\0/g, '\\0')
      .replace(/\t/g, '\\t')
      .replace(/\n/g, '\\n')
      .replace(/\r/g, '\\r')
      .replace(/[\x00-\x0F]/g,          function(ch) { return '\\x0' + hex(ch); })
      .replace(/[\x10-\x1F\x7F-\x9F]/g, function(ch) { return '\\x'  + hex(ch); });
  }

  function classEscape(s) {
    return s
      .replace(/\\/g, '\\\\')
      .replace(/\]/g, '\\]')
      .replace(/\^/g, '\\^')
      .replace(/-/g,  '\\-')
      .replace(/\0/g, '\\0')
      .replace(/\t/g, '\\t')
      .replace(/\n/g, '\\n')
      .replace(/\r/g, '\\r')
      .replace(/[\x00-\x0F]/g,          function(ch) { return '\\x0' + hex(ch); })
      .replace(/[\x10-\x1F\x7F-\x9F]/g, function(ch) { return '\\x'  + hex(ch); });
  }

  function describeExpectation(expectation) {
    return DESCRIBE_EXPECTATION_FNS[expectation.type](expectation);
  }

  function describeExpected(expected) {
    var descriptions = new Array(expected.length),
        i, j;

    for (i = 0; i < expected.length; i++) {
      descriptions[i] = describeExpectation(expected[i]);
    }

    descriptions.sort();

    if (descriptions.length > 0) {
      for (i = 1, j = 1; i < descriptions.length; i++) {
        if (descriptions[i - 1] !== descriptions[i]) {
          descriptions[j] = descriptions[i];
          j++;
        }
      }
      descriptions.length = j;
    }

    switch (descriptions.length) {
      case 1:
        return descriptions[0];

      case 2:
        return descriptions[0] + " or " + descriptions[1];

      default:
        return descriptions.slice(0, -1).join(", ")
          + ", or "
          + descriptions[descriptions.length - 1];
    }
  }

  function describeFound(found) {
    return found ? "\"" + literalEscape(found) + "\"" : "end of input";
  }

  return "Expected " + describeExpected(expected) + " but " + describeFound(found) + " found.";
};

function peg$parse(input, options) {
  options = options !== void 0 ? options : {};

  var peg$FAILED = {},

      peg$startRuleFunctions = { start: peg$parsestart, Expression: peg$parseExpression },
      peg$startRuleFunction  = peg$parsestart,

      peg$c0 = function(ast) { return ast },
      peg$c1 = function(procs) {
            let filt = {"op": "FilterProc", "filter": {"op": "MatchAll"}};
            return {"op": "SequentialProc", "procs": [filt, ... procs]}
          },
      peg$c2 = function(s, rest) {
            if (rest.length == 0) {
                return s
            } else {
                return {"op": "SequentialProc", "procs": [s, ... rest]}
            }
          },
      peg$c3 = function(s) {
            return {"op": "SequentialProc", "procs": [s]}
          },
      peg$c4 = function(first, rest) {
            if (rest) {
              return [first, ... rest]
            } else {
              return [first]
            }
          },
      peg$c5 = "|",
      peg$c6 = peg$literalExpectation("|", false),
      peg$c7 = function(p) { return p },
      peg$c8 = function(expr) {
            return {"op": "FilterProc", "filter": expr}
          },
      peg$c9 = function(first, rest) {
            return makeChain(first, rest, "LogicalOr")
          },
      peg$c10 = function(t) { return t },
      peg$c11 = function(first, rest) {
            return makeChain(first, rest, "LogicalAnd")
          },
      peg$c12 = function(f) { return f },
      peg$c13 = "!",
      peg$c14 = peg$literalExpectation("!", false),
      peg$c15 = function(e) {
            return {"op": "LogicalNot", "expr": e}
          },
      peg$c16 = "-",
      peg$c17 = peg$literalExpectation("-", false),
      peg$c18 = function(s) { return s },
      peg$c19 = "(",
      peg$c20 = peg$literalExpectation("(", false),
      peg$c21 = ")",
      peg$c22 = peg$literalExpectation(")", false),
      peg$c23 = function(expr) { return expr },
      peg$c24 = "*",
      peg$c25 = peg$literalExpectation("*", false),
      peg$c26 = function(comp, v) {
            return {"op": "CompareAny", "comparator": comp, "recursive": false, "value": v}
          },
      peg$c27 = "**",
      peg$c28 = peg$literalExpectation("**", false),
      peg$c29 = function(comp, v) {
            return {"op": "CompareAny", "comparator": comp, "recursive": true, "value": v}
          },
      peg$c30 = function(f, comp, v) {
            return {"op": "CompareField", "comparator": comp, "field": f, "value": v}
          },
      peg$c31 = function(v) {
            return {"op": "CompareAny", "comparator": "in", "recursive": false, "value": v}
          },
      peg$c32 = function(v, f) {
            return {"op": "CompareField", "comparator": "in", "field": f, "value": v}
          },
      peg$c33 = function(v) {
            return {"op": "Search", "text": text(), "value": v}
          },
      peg$c34 = function(v) {
            let str = v;
            if (str == "*") {
              return {"op": "MatchAll"}
            }
            let literal = {"op": "Literal", "type": "string", "value": v};
            if (reglob$1.IsGlobby(str)) {
              literal["type"] = "regexp";
              literal["value"] = reglob$1.Reglob(str);
            }
            return {"op": "Search", "text": text(), "value": literal}
          },
      peg$c35 = function(i) { return i },
      peg$c36 = function(v) { return v },
      peg$c37 = function(v) {
            return {"op": "Literal", "type": "string", "value": v}
          },
      peg$c38 = function(v) {
            return {"op": "Literal", "type": "regexp", "value": v}
          },
      peg$c39 = function(v) {
            return {"op": "Literal", "type": "port", "value": v}
          },
      peg$c40 = function(v) {
            return {"op": "Literal", "type": "net", "value": v}
          },
      peg$c41 = function(v) {
            return {"op": "Literal", "type": "ip", "value": v}
          },
      peg$c42 = function(v) {
            return {"op": "Literal", "type": "float64", "value": v}
          },
      peg$c43 = function(v) {
            return {"op": "Literal", "type": "int64", "value": v}
          },
      peg$c44 = "true",
      peg$c45 = peg$literalExpectation("true", false),
      peg$c46 = function() { return {"op": "Literal", "type": "bool", "value": "true"} },
      peg$c47 = "false",
      peg$c48 = peg$literalExpectation("false", false),
      peg$c49 = function() { return {"op": "Literal", "type": "bool", "value": "false"} },
      peg$c50 = "null",
      peg$c51 = peg$literalExpectation("null", false),
      peg$c52 = function() { return {"op": "Literal", "type": "null"} },
      peg$c53 = function(first, rest) {
            let fp = {"op": "SequentialProc", "procs": first};
            if (rest) {
              return {"op": "ParallelProc", "procs": [fp, ... rest]}
            } else {
              return fp
            }
          },
      peg$c54 = ";",
      peg$c55 = peg$literalExpectation(";", false),
      peg$c56 = function(ch) { return {"op": "SequentialProc", "procs": ch} },
      peg$c57 = function(proc) {
            return proc
          },
      peg$c58 = "by",
      peg$c59 = peg$literalExpectation("by", true),
      peg$c60 = ",",
      peg$c61 = peg$literalExpectation(",", false),
      peg$c62 = function(first, cl) { return cl },
      peg$c63 = function(first, rest) {
            return [first, ... rest]
          },
      peg$c64 = function(field) { return {"op": "ExpressionAssignment", "target": text(), "expression": field} },
      peg$c65 = "every",
      peg$c66 = peg$literalExpectation("every", true),
      peg$c67 = function(dur) { return dur },
      peg$c68 = "and",
      peg$c69 = peg$literalExpectation("and", true),
      peg$c70 = function() { return text() },
      peg$c71 = "or",
      peg$c72 = peg$literalExpectation("or", true),
      peg$c73 = "in",
      peg$c74 = peg$literalExpectation("in", true),
      peg$c75 = "not",
      peg$c76 = peg$literalExpectation("not", true),
      peg$c77 = /^[A-Za-z_$]/,
      peg$c78 = peg$classExpectation([["A", "Z"], ["a", "z"], "_", "$"], false, false),
      peg$c79 = /^[0-9]/,
      peg$c80 = peg$classExpectation([["0", "9"]], false, false),
      peg$c81 = ".",
      peg$c82 = peg$literalExpectation(".", false),
      peg$c83 = function(base, field) { return {"op": "FieldCall", "fn": "RecordFieldRead", "field": null, "param": field} },
      peg$c84 = "[",
      peg$c85 = peg$literalExpectation("[", false),
      peg$c86 = "]",
      peg$c87 = peg$literalExpectation("]", false),
      peg$c88 = function(base, index) { return {"op": "FieldCall", "fn": "Index", "field": null, "param": index} },
      peg$c89 = function(base, ds) {
           let ret = {"op": "FieldRead", "field": base};
           for(let  d of ds) {
             let derefs = d; 
             derefs["field"] = ret;
             ret = derefs;
           }
           return ret
         },
      peg$c90 = function(fn, field) {
            return {"op": "FieldCall", "fn": fn, "field": field, "param": null}
          },
      peg$c91 = "len",
      peg$c92 = peg$literalExpectation("len", true),
      peg$c93 = function() { return "Len" },
      peg$c94 = function(first, rest) {
            let result = [first];

            for(let  r of rest) {
              result.push( r[3]);
            }

            return result
        },
      peg$c95 = function(base, refs) { return text() },
      peg$c96 = "count",
      peg$c97 = peg$literalExpectation("count", true),
      peg$c98 = function() { return "Count" },
      peg$c99 = "sum",
      peg$c100 = peg$literalExpectation("sum", true),
      peg$c101 = function() { return "Sum" },
      peg$c102 = "avg",
      peg$c103 = peg$literalExpectation("avg", true),
      peg$c104 = function() { return "Avg" },
      peg$c105 = "stdev",
      peg$c106 = peg$literalExpectation("stdev", true),
      peg$c107 = function() { return "Stdev" },
      peg$c108 = "sd",
      peg$c109 = peg$literalExpectation("sd", true),
      peg$c110 = "var",
      peg$c111 = peg$literalExpectation("var", true),
      peg$c112 = function() { return "Var" },
      peg$c113 = "entropy",
      peg$c114 = peg$literalExpectation("entropy", true),
      peg$c115 = function() { return "Entropy" },
      peg$c116 = "min",
      peg$c117 = peg$literalExpectation("min", true),
      peg$c118 = function() { return "Min" },
      peg$c119 = "max",
      peg$c120 = peg$literalExpectation("max", true),
      peg$c121 = function() { return "Max" },
      peg$c122 = "first",
      peg$c123 = peg$literalExpectation("first", true),
      peg$c124 = function() { return "First" },
      peg$c125 = "last",
      peg$c126 = peg$literalExpectation("last", true),
      peg$c127 = function() { return "Last" },
      peg$c128 = "countdistinct",
      peg$c129 = peg$literalExpectation("countdistinct", true),
      peg$c130 = function() { return "CountDistinct" },
      peg$c131 = function(field) { return field },
      peg$c132 = function(op, field) {
          let r = {"op": op, "var": "count"};
          if (field) {
            r["field"] = field;
          }
          return r
        },
      peg$c133 = function(op, field) {
          let r = {"op": op, "var": toLowerCase(op)};
          if (field) {
            r["field"] = field;
          }
          return r
        },
      peg$c134 = function(every, reducers, keys, limit) {
          if (OR(keys, every)) {
            if (keys) {
              keys = keys[1];
            } else {
              keys = [];
            }

            if (every) {
              every = every[0];
            }

            return {"op": "GroupByProc", "duration": every, "limit": limit, "keys": keys, "reducers": reducers}
          }
          return {"op": "GroupByProc", "reducers": reducers}
        },
      peg$c135 = "=",
      peg$c136 = peg$literalExpectation("=", false),
      peg$c137 = function(field, f) {
          let r = f;
          r["var"] = field;    
          return r
        },
      peg$c138 = function(first, rest) {
            let result = [first];
            for(let  r of rest) {
              result.push( r[3]);
            }
            return result
          },
      peg$c139 = "sort",
      peg$c140 = peg$literalExpectation("sort", true),
      peg$c141 = function(args, l) { return l },
      peg$c142 = function(args, list) {
          let argm = args;
          let proc = {"op": "SortProc", "fields": list, "sortdir": 1, "nullsfirst": false};
          if ( "r" in argm) {
            proc["sortdir"] = -1;
          }
          if ( "nulls" in argm) {
            if (argm["nulls"] == "first") {
              proc["nullsfirst"] = true;
            }
          }
          return proc
        },
      peg$c143 = function(a) { return a },
      peg$c144 = function(args) {
          return makeArgMap(args)
      },
      peg$c145 = "-r",
      peg$c146 = peg$literalExpectation("-r", false),
      peg$c147 = function() { return {"name": "r", "value": null} },
      peg$c148 = "-nulls",
      peg$c149 = peg$literalExpectation("-nulls", false),
      peg$c150 = peg$literalExpectation("first", false),
      peg$c151 = peg$literalExpectation("last", false),
      peg$c152 = function(where) { return {"name": "nulls", "value": where} },
      peg$c153 = "top",
      peg$c154 = peg$literalExpectation("top", true),
      peg$c155 = function(n) { return n},
      peg$c156 = "-flush",
      peg$c157 = peg$literalExpectation("-flush", false),
      peg$c158 = function(limit, flush, f) { return f },
      peg$c159 = function(limit, flush, fields) {
          let proc = {"op": "TopProc"};
          if (limit) {
            proc["limit"] = limit;
          }
          if (fields) {
            proc["fields"] = fields;
          }
          if (flush) {
            proc["flush"] = true;
          }
          return proc
        },
      peg$c160 = "-limit",
      peg$c161 = peg$literalExpectation("-limit", false),
      peg$c162 = function(limit) { return limit },
      peg$c163 = "-c",
      peg$c164 = peg$literalExpectation("-c", false),
      peg$c165 = function() { return {"name": "c", "value": null} },
      peg$c166 = function(args) {
          return makeArgMap(args)
        },
      peg$c167 = function(field) {
          return {"target": "", "source": field}
        },
      peg$c168 = "cut",
      peg$c169 = peg$literalExpectation("cut", true),
      peg$c170 = function(args, first, cl) { return cl },
      peg$c171 = function(args, first, rest) {
          let argm = args;
          let proc = {"op": "CutProc", "fields": [first, ... rest], "complement": false}; 
          if ( "c" in argm) {
            proc["complement"] = true;
          }
          return proc
        },
      peg$c172 = "head",
      peg$c173 = peg$literalExpectation("head", true),
      peg$c174 = function(count) { return {"op": "HeadProc", "count": count} },
      peg$c175 = function() { return {"op": "HeadProc", "count": 1} },
      peg$c176 = "tail",
      peg$c177 = peg$literalExpectation("tail", true),
      peg$c178 = function(count) { return {"op": "TailProc", "count": count} },
      peg$c179 = function() { return {"op": "TailProc", "count": 1} },
      peg$c180 = "filter",
      peg$c181 = peg$literalExpectation("filter", true),
      peg$c182 = "uniq",
      peg$c183 = peg$literalExpectation("uniq", true),
      peg$c184 = function() {
            return {"op": "UniqProc", "cflag": true}
          },
      peg$c185 = function() {
            return {"op": "UniqProc", "cflag": false}
          },
      peg$c186 = "put",
      peg$c187 = peg$literalExpectation("put", true),
      peg$c188 = function(first, rest) {
            return {"op": "PutProc", "clauses": [first, ... rest]}
          },
      peg$c189 = "rename",
      peg$c190 = peg$literalExpectation("rename", true),
      peg$c191 = function(first, rest) {
            return {"op": "RenameProc", "fields": [first, ... rest]}
          },
      peg$c192 = function(f, e) {
            return {"target": f, "expression": e}
          },
      peg$c193 = function(l, r) {
            return {"target": l, "source": r}
          },
      peg$c194 = function(f) {
            let ret = {"op": "FieldRead", "field": f};
            for(let  d of []) {
              let derefs = d; 
              derefs["field"] = ret;
              ret = derefs;
            }
            return ret
          },
      peg$c195 = "?",
      peg$c196 = peg$literalExpectation("?", false),
      peg$c197 = ":",
      peg$c198 = peg$literalExpectation(":", false),
      peg$c199 = function(condition, thenClause, elseClause) {
          return {"op": "ConditionalExpr", "condition": condition, "then": thenClause, "else": elseClause}
        },
      peg$c200 = function(first, op, expr) { return [op, expr] },
      peg$c201 = function(first, rest) {
              return makeBinaryExprChain(first, rest)
          },
      peg$c202 = function(first, comp, expr) { return [comp, expr] },
      peg$c203 = "=~",
      peg$c204 = peg$literalExpectation("=~", false),
      peg$c205 = "!~",
      peg$c206 = peg$literalExpectation("!~", false),
      peg$c207 = "!=",
      peg$c208 = peg$literalExpectation("!=", false),
      peg$c209 = peg$literalExpectation("in", false),
      peg$c210 = "<=",
      peg$c211 = peg$literalExpectation("<=", false),
      peg$c212 = "<",
      peg$c213 = peg$literalExpectation("<", false),
      peg$c214 = ">=",
      peg$c215 = peg$literalExpectation(">=", false),
      peg$c216 = ">",
      peg$c217 = peg$literalExpectation(">", false),
      peg$c218 = "+",
      peg$c219 = peg$literalExpectation("+", false),
      peg$c220 = "/",
      peg$c221 = peg$literalExpectation("/", false),
      peg$c222 = function(e) {
              return {"op": "UnaryExpr", "operator": "!", "operand": e}
          },
      peg$c223 = function(e, ct) { return ct },
      peg$c224 = function(e, t) {
          if (t) {
            return {"op": "CastExpr", "expr": e, "type": t}
          } else {
            return e
          }
        },
      peg$c225 = "bool",
      peg$c226 = peg$literalExpectation("bool", false),
      peg$c227 = "byte",
      peg$c228 = peg$literalExpectation("byte", false),
      peg$c229 = "int16",
      peg$c230 = peg$literalExpectation("int16", false),
      peg$c231 = "uint16",
      peg$c232 = peg$literalExpectation("uint16", false),
      peg$c233 = "int32",
      peg$c234 = peg$literalExpectation("int32", false),
      peg$c235 = "uint32",
      peg$c236 = peg$literalExpectation("uint32", false),
      peg$c237 = "int64",
      peg$c238 = peg$literalExpectation("int64", false),
      peg$c239 = "uint64",
      peg$c240 = peg$literalExpectation("uint64", false),
      peg$c241 = "float64",
      peg$c242 = peg$literalExpectation("float64", false),
      peg$c243 = "string",
      peg$c244 = peg$literalExpectation("string", false),
      peg$c245 = "bstring",
      peg$c246 = peg$literalExpectation("bstring", false),
      peg$c247 = "ip",
      peg$c248 = peg$literalExpectation("ip", false),
      peg$c249 = "net",
      peg$c250 = peg$literalExpectation("net", false),
      peg$c251 = "time",
      peg$c252 = peg$literalExpectation("time", false),
      peg$c253 = "duration",
      peg$c254 = peg$literalExpectation("duration", false),
      peg$c255 = function(fn, args) {
              return {"op": "FunctionCall", "function": fn, "args": args}
          },
      peg$c256 = /^[A-Za-z]/,
      peg$c257 = peg$classExpectation([["A", "Z"], ["a", "z"]], false, false),
      peg$c258 = /^[.0-9]/,
      peg$c259 = peg$classExpectation([".", ["0", "9"]], false, false),
      peg$c260 = function(first, e) { return e },
      peg$c261 = function(first, rest) {
            return [first, ... rest]
        },
      peg$c262 = function() { return [] },
      peg$c263 = function(base, index) {
                return ["[", index]
              },
      peg$c264 = function(base, field) {
                return [".", {"op": "Literal", "type": "string", "value": field}]
              },
      peg$c265 = function(base, derefs) {
              return makeBinaryExprChain(base, derefs)
          },
      peg$c266 = peg$literalExpectation("and", false),
      peg$c267 = "seconds",
      peg$c268 = peg$literalExpectation("seconds", false),
      peg$c269 = "second",
      peg$c270 = peg$literalExpectation("second", false),
      peg$c271 = "secs",
      peg$c272 = peg$literalExpectation("secs", false),
      peg$c273 = "sec",
      peg$c274 = peg$literalExpectation("sec", false),
      peg$c275 = "s",
      peg$c276 = peg$literalExpectation("s", false),
      peg$c277 = "minutes",
      peg$c278 = peg$literalExpectation("minutes", false),
      peg$c279 = "minute",
      peg$c280 = peg$literalExpectation("minute", false),
      peg$c281 = "mins",
      peg$c282 = peg$literalExpectation("mins", false),
      peg$c283 = peg$literalExpectation("min", false),
      peg$c284 = "m",
      peg$c285 = peg$literalExpectation("m", false),
      peg$c286 = "hours",
      peg$c287 = peg$literalExpectation("hours", false),
      peg$c288 = "hrs",
      peg$c289 = peg$literalExpectation("hrs", false),
      peg$c290 = "hr",
      peg$c291 = peg$literalExpectation("hr", false),
      peg$c292 = "h",
      peg$c293 = peg$literalExpectation("h", false),
      peg$c294 = "hour",
      peg$c295 = peg$literalExpectation("hour", false),
      peg$c296 = "days",
      peg$c297 = peg$literalExpectation("days", false),
      peg$c298 = "day",
      peg$c299 = peg$literalExpectation("day", false),
      peg$c300 = "d",
      peg$c301 = peg$literalExpectation("d", false),
      peg$c302 = "weeks",
      peg$c303 = peg$literalExpectation("weeks", false),
      peg$c304 = "week",
      peg$c305 = peg$literalExpectation("week", false),
      peg$c306 = "wks",
      peg$c307 = peg$literalExpectation("wks", false),
      peg$c308 = "wk",
      peg$c309 = peg$literalExpectation("wk", false),
      peg$c310 = "w",
      peg$c311 = peg$literalExpectation("w", false),
      peg$c312 = function() { return {"type": "Duration", "seconds": 1} },
      peg$c313 = function(num) { return {"type": "Duration", "seconds": num} },
      peg$c314 = function() { return {"type": "Duration", "seconds": 60} },
      peg$c315 = function(num) { return {"type": "Duration", "seconds": num*60} },
      peg$c316 = function() { return {"type": "Duration", "seconds": 3600} },
      peg$c317 = function(num) { return {"type": "Duration", "seconds": num*3600} },
      peg$c318 = function() { return {"type": "Duration", "seconds": 3600*24} },
      peg$c319 = function(num) { return {"type": "Duration", "seconds": (num*3600*24)} },
      peg$c320 = function(num) { return {"type": "Duration", "seconds": num*3600*24*7} },
      peg$c321 = function(a) { return text() },
      peg$c322 = function(a, b) {
            return joinChars(a) + b
          },
      peg$c323 = "::",
      peg$c324 = peg$literalExpectation("::", false),
      peg$c325 = function(a, b, d, e) {
            return a + joinChars(b) + "::" + joinChars(d) + e
          },
      peg$c326 = function(a, b) {
            return "::" + joinChars(a) + b
          },
      peg$c327 = function(a, b) {
            return a + joinChars(b) + "::"
          },
      peg$c328 = function() {
            return "::"
          },
      peg$c329 = function(v) { return ":" + v },
      peg$c330 = function(v) { return v + ":" },
      peg$c331 = function(a, m) {
            return a + "/" + m.toString();
          },
      peg$c332 = function(a, m) {
            return a + "/" + m;
          },
      peg$c333 = function(s) { return parseInt(s) },
      peg$c334 = /^[+\-]/,
      peg$c335 = peg$classExpectation(["+", "-"], false, false),
      peg$c337 = function() {
            return text()
          },
      peg$c338 = "0",
      peg$c339 = peg$literalExpectation("0", false),
      peg$c340 = /^[1-9]/,
      peg$c341 = peg$classExpectation([["1", "9"]], false, false),
      peg$c342 = "e",
      peg$c343 = peg$literalExpectation("e", true),
      peg$c344 = function(chars) { return text() },
      peg$c345 = /^[0-9a-fA-F]/,
      peg$c346 = peg$classExpectation([["0", "9"], ["a", "f"], ["A", "F"]], false, false),
      peg$c347 = function(chars) { return joinChars(chars) },
      peg$c348 = "\\",
      peg$c349 = peg$literalExpectation("\\", false),
      peg$c350 = /^[\0-\x1F\\(),!><="|';]/,
      peg$c351 = peg$classExpectation([["\0", "\x1F"], "\\", "(", ")", ",", "!", ">", "<", "=", "\"", "|", "'", ";"], false, false),
      peg$c352 = peg$anyExpectation(),
      peg$c353 = "\"",
      peg$c354 = peg$literalExpectation("\"", false),
      peg$c355 = function(v) { return joinChars(v) },
      peg$c356 = "'",
      peg$c357 = peg$literalExpectation("'", false),
      peg$c358 = "x",
      peg$c359 = peg$literalExpectation("x", false),
      peg$c360 = function() { return "\\" + text() },
      peg$c361 = "b",
      peg$c362 = peg$literalExpectation("b", false),
      peg$c363 = function() { return "\b" },
      peg$c364 = "f",
      peg$c365 = peg$literalExpectation("f", false),
      peg$c366 = function() { return "\f" },
      peg$c367 = "n",
      peg$c368 = peg$literalExpectation("n", false),
      peg$c369 = function() { return "\n" },
      peg$c370 = "r",
      peg$c371 = peg$literalExpectation("r", false),
      peg$c372 = function() { return "\r" },
      peg$c373 = "t",
      peg$c374 = peg$literalExpectation("t", false),
      peg$c375 = function() { return "\t" },
      peg$c376 = "v",
      peg$c377 = peg$literalExpectation("v", false),
      peg$c378 = function() { return "\v" },
      peg$c379 = function() { return "=" },
      peg$c380 = function() { return "\\*" },
      peg$c381 = "u",
      peg$c382 = peg$literalExpectation("u", false),
      peg$c383 = function(chars) {
            return makeUnicodeChar(chars)
          },
      peg$c384 = "{",
      peg$c385 = peg$literalExpectation("{", false),
      peg$c386 = "}",
      peg$c387 = peg$literalExpectation("}", false),
      peg$c388 = /^[^\/\\]/,
      peg$c389 = peg$classExpectation(["/", "\\"], true, false),
      peg$c390 = "\\/",
      peg$c391 = peg$literalExpectation("\\/", false),
      peg$c392 = /^[\0-\x1F\\]/,
      peg$c393 = peg$classExpectation([["\0", "\x1F"], "\\"], false, false),
      peg$c394 = "\t",
      peg$c395 = peg$literalExpectation("\t", false),
      peg$c396 = "\x0B",
      peg$c397 = peg$literalExpectation("\x0B", false),
      peg$c398 = "\f",
      peg$c399 = peg$literalExpectation("\f", false),
      peg$c400 = " ",
      peg$c401 = peg$literalExpectation(" ", false),
      peg$c402 = "\xA0",
      peg$c403 = peg$literalExpectation("\xA0", false),
      peg$c404 = "\uFEFF",
      peg$c405 = peg$literalExpectation("\uFEFF", false),
      peg$c406 = peg$otherExpectation("whitespace"),

      peg$currPos          = 0,
      peg$savedPos         = 0,
      peg$posDetailsCache  = [{ line: 1, column: 1 }],
      peg$maxFailPos       = 0,
      peg$maxFailExpected  = [],
      peg$silentFails      = 0,

      peg$result;

  if ("startRule" in options) {
    if (!(options.startRule in peg$startRuleFunctions)) {
      throw new Error("Can't start parsing from rule \"" + options.startRule + "\".");
    }

    peg$startRuleFunction = peg$startRuleFunctions[options.startRule];
  }

  function text() {
    return input.substring(peg$savedPos, peg$currPos);
  }

  function peg$literalExpectation(text, ignoreCase) {
    return { type: "literal", text: text, ignoreCase: ignoreCase };
  }

  function peg$classExpectation(parts, inverted, ignoreCase) {
    return { type: "class", parts: parts, inverted: inverted, ignoreCase: ignoreCase };
  }

  function peg$anyExpectation() {
    return { type: "any" };
  }

  function peg$endExpectation() {
    return { type: "end" };
  }

  function peg$otherExpectation(description) {
    return { type: "other", description: description };
  }

  function peg$computePosDetails(pos) {
    var details = peg$posDetailsCache[pos], p;

    if (details) {
      return details;
    } else {
      p = pos - 1;
      while (!peg$posDetailsCache[p]) {
        p--;
      }

      details = peg$posDetailsCache[p];
      details = {
        line:   details.line,
        column: details.column
      };

      while (p < pos) {
        if (input.charCodeAt(p) === 10) {
          details.line++;
          details.column = 1;
        } else {
          details.column++;
        }

        p++;
      }

      peg$posDetailsCache[pos] = details;
      return details;
    }
  }

  function peg$computeLocation(startPos, endPos) {
    var startPosDetails = peg$computePosDetails(startPos),
        endPosDetails   = peg$computePosDetails(endPos);

    return {
      start: {
        offset: startPos,
        line:   startPosDetails.line,
        column: startPosDetails.column
      },
      end: {
        offset: endPos,
        line:   endPosDetails.line,
        column: endPosDetails.column
      }
    };
  }

  function peg$fail(expected) {
    if (peg$currPos < peg$maxFailPos) { return; }

    if (peg$currPos > peg$maxFailPos) {
      peg$maxFailPos = peg$currPos;
      peg$maxFailExpected = [];
    }

    peg$maxFailExpected.push(expected);
  }

  function peg$buildStructuredError(expected, found, location) {
    return new peg$SyntaxError(
      peg$SyntaxError.buildMessage(expected, found),
      expected,
      found,
      location
    );
  }

  function peg$parsestart() {
    var s0, s1, s2, s3, s4;

    s0 = peg$currPos;
    s1 = peg$parse_();
    if (s1 === peg$FAILED) {
      s1 = null;
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parsequery();
      if (s2 !== peg$FAILED) {
        s3 = peg$parse_();
        if (s3 === peg$FAILED) {
          s3 = null;
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parseEOF();
          if (s4 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c0(s2);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsequery() {
    var s0, s1, s2, s3, s4;

    s0 = peg$currPos;
    s1 = peg$parseprocChain();
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c1(s1);
    }
    s0 = s1;
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      s1 = peg$parsesearch();
      if (s1 !== peg$FAILED) {
        s2 = peg$parse_();
        if (s2 === peg$FAILED) {
          s2 = null;
        }
        if (s2 !== peg$FAILED) {
          s3 = [];
          s4 = peg$parsechainedProc();
          while (s4 !== peg$FAILED) {
            s3.push(s4);
            s4 = peg$parsechainedProc();
          }
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c2(s1, s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parsesearch();
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c3(s1);
        }
        s0 = s1;
      }
    }

    return s0;
  }

  function peg$parseprocChain() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    s1 = peg$parseproc();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$parsechainedProc();
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$parsechainedProc();
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c4(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsechainedProc() {
    var s0, s1, s2, s3, s4;

    s0 = peg$currPos;
    s1 = peg$parse_();
    if (s1 === peg$FAILED) {
      s1 = null;
    }
    if (s1 !== peg$FAILED) {
      if (input.charCodeAt(peg$currPos) === 124) {
        s2 = peg$c5;
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c6); }
      }
      if (s2 !== peg$FAILED) {
        s3 = peg$parse_();
        if (s3 === peg$FAILED) {
          s3 = null;
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parseproc();
          if (s4 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c7(s4);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsesearch() {
    var s0, s1;

    s0 = peg$currPos;
    s1 = peg$parsesearchExpr();
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c8(s1);
    }
    s0 = s1;

    return s0;
  }

  function peg$parsesearchExpr() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    s1 = peg$parsesearchTerm();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$parseoredSearchTerm();
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$parseoredSearchTerm();
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c9(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseoredSearchTerm() {
    var s0, s1, s2, s3, s4;

    s0 = peg$currPos;
    s1 = peg$parse_();
    if (s1 !== peg$FAILED) {
      s2 = peg$parseorToken();
      if (s2 !== peg$FAILED) {
        s3 = peg$parse_();
        if (s3 !== peg$FAILED) {
          s4 = peg$parsesearchTerm();
          if (s4 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c10(s4);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsesearchTerm() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    s1 = peg$parsesearchFactor();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$parseandedSearchTerm();
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$parseandedSearchTerm();
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c11(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseandedSearchTerm() {
    var s0, s1, s2, s3, s4;

    s0 = peg$currPos;
    s1 = peg$parse_();
    if (s1 !== peg$FAILED) {
      s2 = peg$currPos;
      s3 = peg$parseandToken();
      if (s3 !== peg$FAILED) {
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          s3 = [s3, s4];
          s2 = s3;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
      } else {
        peg$currPos = s2;
        s2 = peg$FAILED;
      }
      if (s2 === peg$FAILED) {
        s2 = null;
      }
      if (s2 !== peg$FAILED) {
        s3 = peg$parsesearchFactor();
        if (s3 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c12(s3);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsesearchFactor() {
    var s0, s1, s2, s3, s4, s5;

    s0 = peg$currPos;
    s1 = peg$currPos;
    s2 = peg$parsenotToken();
    if (s2 !== peg$FAILED) {
      s3 = peg$parse_();
      if (s3 !== peg$FAILED) {
        s2 = [s2, s3];
        s1 = s2;
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
    } else {
      peg$currPos = s1;
      s1 = peg$FAILED;
    }
    if (s1 === peg$FAILED) {
      s1 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 33) {
        s2 = peg$c13;
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c14); }
      }
      if (s2 !== peg$FAILED) {
        s3 = peg$parse_();
        if (s3 === peg$FAILED) {
          s3 = null;
        }
        if (s3 !== peg$FAILED) {
          s2 = [s2, s3];
          s1 = s2;
        } else {
          peg$currPos = s1;
          s1 = peg$FAILED;
        }
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parsesearchExpr();
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c15(s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      s1 = peg$currPos;
      peg$silentFails++;
      if (input.charCodeAt(peg$currPos) === 45) {
        s2 = peg$c16;
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c17); }
      }
      peg$silentFails--;
      if (s2 === peg$FAILED) {
        s1 = void 0;
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$parsesearchPred();
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c18(s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 40) {
          s1 = peg$c19;
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c20); }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$parse_();
          if (s2 === peg$FAILED) {
            s2 = null;
          }
          if (s2 !== peg$FAILED) {
            s3 = peg$parsesearchExpr();
            if (s3 !== peg$FAILED) {
              s4 = peg$parse_();
              if (s4 === peg$FAILED) {
                s4 = null;
              }
              if (s4 !== peg$FAILED) {
                if (input.charCodeAt(peg$currPos) === 41) {
                  s5 = peg$c21;
                  peg$currPos++;
                } else {
                  s5 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c22); }
                }
                if (s5 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$c23(s3);
                  s0 = s1;
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      }
    }

    return s0;
  }

  function peg$parsesearchPred() {
    var s0, s1, s2, s3, s4, s5;

    s0 = peg$currPos;
    if (input.charCodeAt(peg$currPos) === 42) {
      s1 = peg$c24;
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c25); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parse_();
      if (s2 === peg$FAILED) {
        s2 = null;
      }
      if (s2 !== peg$FAILED) {
        s3 = peg$parseequalityToken();
        if (s3 !== peg$FAILED) {
          s4 = peg$parse_();
          if (s4 === peg$FAILED) {
            s4 = null;
          }
          if (s4 !== peg$FAILED) {
            s5 = peg$parsesearchValue();
            if (s5 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c26(s3, s5);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      if (input.substr(peg$currPos, 2) === peg$c27) {
        s1 = peg$c27;
        peg$currPos += 2;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c28); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$parse_();
        if (s2 === peg$FAILED) {
          s2 = null;
        }
        if (s2 !== peg$FAILED) {
          s3 = peg$parseequalityToken();
          if (s3 !== peg$FAILED) {
            s4 = peg$parse_();
            if (s4 === peg$FAILED) {
              s4 = null;
            }
            if (s4 !== peg$FAILED) {
              s5 = peg$parsesearchValue();
              if (s5 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c29(s3, s5);
                s0 = s1;
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parsefieldExpr();
        if (s1 !== peg$FAILED) {
          s2 = peg$parse_();
          if (s2 === peg$FAILED) {
            s2 = null;
          }
          if (s2 !== peg$FAILED) {
            s3 = peg$parseequalityToken();
            if (s3 !== peg$FAILED) {
              s4 = peg$parse_();
              if (s4 === peg$FAILED) {
                s4 = null;
              }
              if (s4 !== peg$FAILED) {
                s5 = peg$parsesearchValue();
                if (s5 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$c30(s1, s3, s5);
                  s0 = s1;
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parsesearchValue();
          if (s1 !== peg$FAILED) {
            s2 = peg$parse_();
            if (s2 === peg$FAILED) {
              s2 = null;
            }
            if (s2 !== peg$FAILED) {
              s3 = peg$parseinToken();
              if (s3 !== peg$FAILED) {
                s4 = peg$parse_();
                if (s4 === peg$FAILED) {
                  s4 = null;
                }
                if (s4 !== peg$FAILED) {
                  if (input.charCodeAt(peg$currPos) === 42) {
                    s5 = peg$c24;
                    peg$currPos++;
                  } else {
                    s5 = peg$FAILED;
                    if (peg$silentFails === 0) { peg$fail(peg$c25); }
                  }
                  if (s5 !== peg$FAILED) {
                    peg$savedPos = s0;
                    s1 = peg$c31(s1);
                    s0 = s1;
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            s1 = peg$parsesearchValue();
            if (s1 !== peg$FAILED) {
              s2 = peg$parse_();
              if (s2 === peg$FAILED) {
                s2 = null;
              }
              if (s2 !== peg$FAILED) {
                s3 = peg$parseinToken();
                if (s3 !== peg$FAILED) {
                  s4 = peg$parse_();
                  if (s4 === peg$FAILED) {
                    s4 = null;
                  }
                  if (s4 !== peg$FAILED) {
                    s5 = peg$parsefieldReference();
                    if (s5 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s1 = peg$c32(s1, s5);
                      s0 = s1;
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
            if (s0 === peg$FAILED) {
              s0 = peg$currPos;
              s1 = peg$parsesearchLiteral();
              if (s1 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c33(s1);
              }
              s0 = s1;
              if (s0 === peg$FAILED) {
                s0 = peg$currPos;
                s1 = peg$currPos;
                peg$silentFails++;
                s2 = peg$currPos;
                s3 = peg$parsesearchKeywords();
                if (s3 !== peg$FAILED) {
                  s4 = peg$parse_();
                  if (s4 !== peg$FAILED) {
                    s3 = [s3, s4];
                    s2 = s3;
                  } else {
                    peg$currPos = s2;
                    s2 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s2;
                  s2 = peg$FAILED;
                }
                peg$silentFails--;
                if (s2 === peg$FAILED) {
                  s1 = void 0;
                } else {
                  peg$currPos = s1;
                  s1 = peg$FAILED;
                }
                if (s1 !== peg$FAILED) {
                  s2 = peg$parsesearchWord();
                  if (s2 !== peg$FAILED) {
                    peg$savedPos = s0;
                    s1 = peg$c34(s2);
                    s0 = s1;
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              }
            }
          }
        }
      }
    }

    return s0;
  }

  function peg$parsesearchLiteral() {
    var s0, s1, s2, s3, s4;

    s0 = peg$parseStringLiteral();
    if (s0 === peg$FAILED) {
      s0 = peg$parseRegexpLiteral();
      if (s0 === peg$FAILED) {
        s0 = peg$parsePortLiteral();
        if (s0 === peg$FAILED) {
          s0 = peg$parseSubnetLiteral();
          if (s0 === peg$FAILED) {
            s0 = peg$parseAddressLiteral();
            if (s0 === peg$FAILED) {
              s0 = peg$parseFloatLiteral();
              if (s0 === peg$FAILED) {
                s0 = peg$currPos;
                s1 = peg$parseIntegerLiteral();
                if (s1 !== peg$FAILED) {
                  s2 = peg$currPos;
                  peg$silentFails++;
                  s3 = peg$parsesearchWord();
                  peg$silentFails--;
                  if (s3 === peg$FAILED) {
                    s2 = void 0;
                  } else {
                    peg$currPos = s2;
                    s2 = peg$FAILED;
                  }
                  if (s2 !== peg$FAILED) {
                    peg$savedPos = s0;
                    s1 = peg$c35(s1);
                    s0 = s1;
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
                if (s0 === peg$FAILED) {
                  s0 = peg$currPos;
                  s1 = peg$currPos;
                  peg$silentFails++;
                  s2 = peg$currPos;
                  s3 = peg$parsesearchKeywords();
                  if (s3 !== peg$FAILED) {
                    s4 = peg$parse_();
                    if (s4 !== peg$FAILED) {
                      s3 = [s3, s4];
                      s2 = s3;
                    } else {
                      peg$currPos = s2;
                      s2 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s2;
                    s2 = peg$FAILED;
                  }
                  peg$silentFails--;
                  if (s2 === peg$FAILED) {
                    s1 = void 0;
                  } else {
                    peg$currPos = s1;
                    s1 = peg$FAILED;
                  }
                  if (s1 !== peg$FAILED) {
                    s2 = peg$parseBooleanLiteral();
                    if (s2 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s1 = peg$c36(s2);
                      s0 = s1;
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                  if (s0 === peg$FAILED) {
                    s0 = peg$currPos;
                    s1 = peg$currPos;
                    peg$silentFails++;
                    s2 = peg$currPos;
                    s3 = peg$parsesearchKeywords();
                    if (s3 !== peg$FAILED) {
                      s4 = peg$parse_();
                      if (s4 !== peg$FAILED) {
                        s3 = [s3, s4];
                        s2 = s3;
                      } else {
                        peg$currPos = s2;
                        s2 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s2;
                      s2 = peg$FAILED;
                    }
                    peg$silentFails--;
                    if (s2 === peg$FAILED) {
                      s1 = void 0;
                    } else {
                      peg$currPos = s1;
                      s1 = peg$FAILED;
                    }
                    if (s1 !== peg$FAILED) {
                      s2 = peg$parseNullLiteral();
                      if (s2 !== peg$FAILED) {
                        peg$savedPos = s0;
                        s1 = peg$c36(s2);
                        s0 = s1;
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    return s0;
  }

  function peg$parsesearchValue() {
    var s0, s1, s2, s3, s4;

    s0 = peg$parsesearchLiteral();
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      s1 = peg$currPos;
      peg$silentFails++;
      s2 = peg$currPos;
      s3 = peg$parsesearchKeywords();
      if (s3 !== peg$FAILED) {
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          s3 = [s3, s4];
          s2 = s3;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
      } else {
        peg$currPos = s2;
        s2 = peg$FAILED;
      }
      peg$silentFails--;
      if (s2 === peg$FAILED) {
        s1 = void 0;
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$parsesearchWord();
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c37(s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    }

    return s0;
  }

  function peg$parseStringLiteral() {
    var s0, s1;

    s0 = peg$currPos;
    s1 = peg$parsequotedString();
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c37(s1);
    }
    s0 = s1;

    return s0;
  }

  function peg$parseRegexpLiteral() {
    var s0, s1;

    s0 = peg$currPos;
    s1 = peg$parsereString();
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c38(s1);
    }
    s0 = s1;

    return s0;
  }

  function peg$parsePortLiteral() {
    var s0, s1;

    s0 = peg$currPos;
    s1 = peg$parseport();
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c39(s1);
    }
    s0 = s1;

    return s0;
  }

  function peg$parseSubnetLiteral() {
    var s0, s1;

    s0 = peg$currPos;
    s1 = peg$parseip6subnet();
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c40(s1);
    }
    s0 = s1;
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      s1 = peg$parsesubnet();
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c40(s1);
      }
      s0 = s1;
    }

    return s0;
  }

  function peg$parseAddressLiteral() {
    var s0, s1;

    s0 = peg$currPos;
    s1 = peg$parseip6addr();
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c41(s1);
    }
    s0 = s1;
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      s1 = peg$parseaddr();
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c41(s1);
      }
      s0 = s1;
    }

    return s0;
  }

  function peg$parseFloatLiteral() {
    var s0, s1;

    s0 = peg$currPos;
    s1 = peg$parsesdouble();
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c42(s1);
    }
    s0 = s1;

    return s0;
  }

  function peg$parseIntegerLiteral() {
    var s0, s1;

    s0 = peg$currPos;
    s1 = peg$parsesinteger();
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c43(s1);
    }
    s0 = s1;

    return s0;
  }

  function peg$parseBooleanLiteral() {
    var s0, s1;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 4) === peg$c44) {
      s1 = peg$c44;
      peg$currPos += 4;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c45); }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c46();
    }
    s0 = s1;
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      if (input.substr(peg$currPos, 5) === peg$c47) {
        s1 = peg$c47;
        peg$currPos += 5;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c48); }
      }
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c49();
      }
      s0 = s1;
    }

    return s0;
  }

  function peg$parseNullLiteral() {
    var s0, s1;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 4) === peg$c50) {
      s1 = peg$c50;
      peg$currPos += 4;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c51); }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c52();
    }
    s0 = s1;

    return s0;
  }

  function peg$parsesearchKeywords() {
    var s0;

    s0 = peg$parseandToken();
    if (s0 === peg$FAILED) {
      s0 = peg$parseorToken();
      if (s0 === peg$FAILED) {
        s0 = peg$parseinToken();
      }
    }

    return s0;
  }

  function peg$parseprocList() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    s1 = peg$parseprocChain();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$parseparallelChain();
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$parseparallelChain();
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c53(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseparallelChain() {
    var s0, s1, s2, s3, s4;

    s0 = peg$currPos;
    s1 = peg$parse_();
    if (s1 === peg$FAILED) {
      s1 = null;
    }
    if (s1 !== peg$FAILED) {
      if (input.charCodeAt(peg$currPos) === 59) {
        s2 = peg$c54;
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c55); }
      }
      if (s2 !== peg$FAILED) {
        s3 = peg$parse_();
        if (s3 === peg$FAILED) {
          s3 = null;
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parseprocChain();
          if (s4 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c56(s4);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseproc() {
    var s0, s1, s2, s3, s4, s5;

    s0 = peg$parsesimpleProc();
    if (s0 === peg$FAILED) {
      s0 = peg$parsegroupByProc();
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 40) {
          s1 = peg$c19;
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c20); }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$parse_();
          if (s2 === peg$FAILED) {
            s2 = null;
          }
          if (s2 !== peg$FAILED) {
            s3 = peg$parseprocList();
            if (s3 !== peg$FAILED) {
              s4 = peg$parse_();
              if (s4 === peg$FAILED) {
                s4 = null;
              }
              if (s4 !== peg$FAILED) {
                if (input.charCodeAt(peg$currPos) === 41) {
                  s5 = peg$c21;
                  peg$currPos++;
                } else {
                  s5 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c22); }
                }
                if (s5 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$c57(s3);
                  s0 = s1;
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      }
    }

    return s0;
  }

  function peg$parsegroupByKeys() {
    var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 2).toLowerCase() === peg$c58) {
      s1 = input.substr(peg$currPos, 2);
      peg$currPos += 2;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c59); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parse_();
      if (s2 !== peg$FAILED) {
        s3 = peg$parsegroupByKey();
        if (s3 !== peg$FAILED) {
          s4 = [];
          s5 = peg$currPos;
          s6 = peg$parse__();
          if (s6 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 44) {
              s7 = peg$c60;
              peg$currPos++;
            } else {
              s7 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c61); }
            }
            if (s7 !== peg$FAILED) {
              s8 = peg$parse__();
              if (s8 !== peg$FAILED) {
                s9 = peg$parsegroupByKey();
                if (s9 !== peg$FAILED) {
                  peg$savedPos = s5;
                  s6 = peg$c62(s3, s9);
                  s5 = s6;
                } else {
                  peg$currPos = s5;
                  s5 = peg$FAILED;
                }
              } else {
                peg$currPos = s5;
                s5 = peg$FAILED;
              }
            } else {
              peg$currPos = s5;
              s5 = peg$FAILED;
            }
          } else {
            peg$currPos = s5;
            s5 = peg$FAILED;
          }
          while (s5 !== peg$FAILED) {
            s4.push(s5);
            s5 = peg$currPos;
            s6 = peg$parse__();
            if (s6 !== peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 44) {
                s7 = peg$c60;
                peg$currPos++;
              } else {
                s7 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c61); }
              }
              if (s7 !== peg$FAILED) {
                s8 = peg$parse__();
                if (s8 !== peg$FAILED) {
                  s9 = peg$parsegroupByKey();
                  if (s9 !== peg$FAILED) {
                    peg$savedPos = s5;
                    s6 = peg$c62(s3, s9);
                    s5 = s6;
                  } else {
                    peg$currPos = s5;
                    s5 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s5;
                  s5 = peg$FAILED;
                }
              } else {
                peg$currPos = s5;
                s5 = peg$FAILED;
              }
            } else {
              peg$currPos = s5;
              s5 = peg$FAILED;
            }
          }
          if (s4 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c63(s3, s4);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsegroupByKey() {
    var s0, s1;

    s0 = peg$parseExpressionAssignment();
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      s1 = peg$parsefieldExpr();
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c64(s1);
      }
      s0 = s1;
    }

    return s0;
  }

  function peg$parseeveryDur() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 5).toLowerCase() === peg$c65) {
      s1 = input.substr(peg$currPos, 5);
      peg$currPos += 5;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c66); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parse_();
      if (s2 !== peg$FAILED) {
        s3 = peg$parseduration();
        if (s3 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c67(s3);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseequalityToken() {
    var s0;

    s0 = peg$parseEqualityOperator();
    if (s0 === peg$FAILED) {
      s0 = peg$parseRelativeOperator();
    }

    return s0;
  }

  function peg$parseandToken() {
    var s0, s1;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 3).toLowerCase() === peg$c68) {
      s1 = input.substr(peg$currPos, 3);
      peg$currPos += 3;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c69); }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c70();
    }
    s0 = s1;

    return s0;
  }

  function peg$parseorToken() {
    var s0, s1;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 2).toLowerCase() === peg$c71) {
      s1 = input.substr(peg$currPos, 2);
      peg$currPos += 2;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c72); }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c70();
    }
    s0 = s1;

    return s0;
  }

  function peg$parseinToken() {
    var s0, s1;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 2).toLowerCase() === peg$c73) {
      s1 = input.substr(peg$currPos, 2);
      peg$currPos += 2;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c74); }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c70();
    }
    s0 = s1;

    return s0;
  }

  function peg$parsenotToken() {
    var s0, s1;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 3).toLowerCase() === peg$c75) {
      s1 = input.substr(peg$currPos, 3);
      peg$currPos += 3;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c76); }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c70();
    }
    s0 = s1;

    return s0;
  }

  function peg$parsefieldName() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    s1 = peg$parsefieldNameStart();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$parsefieldNameRest();
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$parsefieldNameRest();
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c70();
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsefieldNameStart() {
    var s0;

    if (peg$c77.test(input.charAt(peg$currPos))) {
      s0 = input.charAt(peg$currPos);
      peg$currPos++;
    } else {
      s0 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c78); }
    }

    return s0;
  }

  function peg$parsefieldNameRest() {
    var s0;

    s0 = peg$parsefieldNameStart();
    if (s0 === peg$FAILED) {
      if (peg$c79.test(input.charAt(peg$currPos))) {
        s0 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c80); }
      }
    }

    return s0;
  }

  function peg$parsefieldReference() {
    var s0, s1, s2, s3, s4, s5, s6;

    s0 = peg$currPos;
    s1 = peg$parsefieldName();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 46) {
        s4 = peg$c81;
        peg$currPos++;
      } else {
        s4 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c82); }
      }
      if (s4 !== peg$FAILED) {
        s5 = peg$parsefieldName();
        if (s5 !== peg$FAILED) {
          peg$savedPos = s3;
          s4 = peg$c83(s1, s5);
          s3 = s4;
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      } else {
        peg$currPos = s3;
        s3 = peg$FAILED;
      }
      if (s3 === peg$FAILED) {
        s3 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 91) {
          s4 = peg$c84;
          peg$currPos++;
        } else {
          s4 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c85); }
        }
        if (s4 !== peg$FAILED) {
          s5 = peg$parsesuint();
          if (s5 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 93) {
              s6 = peg$c86;
              peg$currPos++;
            } else {
              s6 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c87); }
            }
            if (s6 !== peg$FAILED) {
              peg$savedPos = s3;
              s4 = peg$c88(s1, s5);
              s3 = s4;
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      }
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 46) {
          s4 = peg$c81;
          peg$currPos++;
        } else {
          s4 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c82); }
        }
        if (s4 !== peg$FAILED) {
          s5 = peg$parsefieldName();
          if (s5 !== peg$FAILED) {
            peg$savedPos = s3;
            s4 = peg$c83(s1, s5);
            s3 = s4;
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        if (s3 === peg$FAILED) {
          s3 = peg$currPos;
          if (input.charCodeAt(peg$currPos) === 91) {
            s4 = peg$c84;
            peg$currPos++;
          } else {
            s4 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c85); }
          }
          if (s4 !== peg$FAILED) {
            s5 = peg$parsesuint();
            if (s5 !== peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 93) {
                s6 = peg$c86;
                peg$currPos++;
              } else {
                s6 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c87); }
              }
              if (s6 !== peg$FAILED) {
                peg$savedPos = s3;
                s4 = peg$c88(s1, s5);
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        }
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c89(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsefieldExpr() {
    var s0, s1, s2, s3, s4, s5, s6, s7;

    s0 = peg$currPos;
    s1 = peg$parsefieldOp();
    if (s1 !== peg$FAILED) {
      s2 = peg$parse_();
      if (s2 === peg$FAILED) {
        s2 = null;
      }
      if (s2 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 40) {
          s3 = peg$c19;
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c20); }
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parse_();
          if (s4 === peg$FAILED) {
            s4 = null;
          }
          if (s4 !== peg$FAILED) {
            s5 = peg$parsefieldReference();
            if (s5 !== peg$FAILED) {
              s6 = peg$parse_();
              if (s6 === peg$FAILED) {
                s6 = null;
              }
              if (s6 !== peg$FAILED) {
                if (input.charCodeAt(peg$currPos) === 41) {
                  s7 = peg$c21;
                  peg$currPos++;
                } else {
                  s7 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c22); }
                }
                if (s7 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$c90(s1, s5);
                  s0 = s1;
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$parsefieldReference();
    }

    return s0;
  }

  function peg$parsefieldOp() {
    var s0, s1;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 3).toLowerCase() === peg$c91) {
      s1 = input.substr(peg$currPos, 3);
      peg$currPos += 3;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c92); }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c93();
    }
    s0 = s1;

    return s0;
  }

  function peg$parsefieldExprList() {
    var s0, s1, s2, s3, s4, s5, s6, s7;

    s0 = peg$currPos;
    s1 = peg$parsefieldExpr();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$currPos;
      s4 = peg$parse_();
      if (s4 === peg$FAILED) {
        s4 = null;
      }
      if (s4 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 44) {
          s5 = peg$c60;
          peg$currPos++;
        } else {
          s5 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c61); }
        }
        if (s5 !== peg$FAILED) {
          s6 = peg$parse_();
          if (s6 === peg$FAILED) {
            s6 = null;
          }
          if (s6 !== peg$FAILED) {
            s7 = peg$parsefieldExpr();
            if (s7 !== peg$FAILED) {
              s4 = [s4, s5, s6, s7];
              s3 = s4;
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      } else {
        peg$currPos = s3;
        s3 = peg$FAILED;
      }
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 === peg$FAILED) {
          s4 = null;
        }
        if (s4 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 44) {
            s5 = peg$c60;
            peg$currPos++;
          } else {
            s5 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c61); }
          }
          if (s5 !== peg$FAILED) {
            s6 = peg$parse_();
            if (s6 === peg$FAILED) {
              s6 = null;
            }
            if (s6 !== peg$FAILED) {
              s7 = peg$parsefieldExpr();
              if (s7 !== peg$FAILED) {
                s4 = [s4, s5, s6, s7];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c94(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsefieldRefDotOnly() {
    var s0, s1, s2, s3, s4, s5;

    s0 = peg$currPos;
    s1 = peg$parsefieldName();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 46) {
        s4 = peg$c81;
        peg$currPos++;
      } else {
        s4 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c82); }
      }
      if (s4 !== peg$FAILED) {
        s5 = peg$parsefieldName();
        if (s5 !== peg$FAILED) {
          s4 = [s4, s5];
          s3 = s4;
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      } else {
        peg$currPos = s3;
        s3 = peg$FAILED;
      }
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 46) {
          s4 = peg$c81;
          peg$currPos++;
        } else {
          s4 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c82); }
        }
        if (s4 !== peg$FAILED) {
          s5 = peg$parsefieldName();
          if (s5 !== peg$FAILED) {
            s4 = [s4, s5];
            s3 = s4;
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c95();
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsecountOp() {
    var s0, s1;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 5).toLowerCase() === peg$c96) {
      s1 = input.substr(peg$currPos, 5);
      peg$currPos += 5;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c97); }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c98();
    }
    s0 = s1;

    return s0;
  }

  function peg$parsefieldReducerOp() {
    var s0, s1;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 3).toLowerCase() === peg$c99) {
      s1 = input.substr(peg$currPos, 3);
      peg$currPos += 3;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c100); }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c101();
    }
    s0 = s1;
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      if (input.substr(peg$currPos, 3).toLowerCase() === peg$c102) {
        s1 = input.substr(peg$currPos, 3);
        peg$currPos += 3;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c103); }
      }
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c104();
      }
      s0 = s1;
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        if (input.substr(peg$currPos, 5).toLowerCase() === peg$c105) {
          s1 = input.substr(peg$currPos, 5);
          peg$currPos += 5;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c106); }
        }
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c107();
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          if (input.substr(peg$currPos, 2).toLowerCase() === peg$c108) {
            s1 = input.substr(peg$currPos, 2);
            peg$currPos += 2;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c109); }
          }
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c107();
          }
          s0 = s1;
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            if (input.substr(peg$currPos, 3).toLowerCase() === peg$c110) {
              s1 = input.substr(peg$currPos, 3);
              peg$currPos += 3;
            } else {
              s1 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c111); }
            }
            if (s1 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c112();
            }
            s0 = s1;
            if (s0 === peg$FAILED) {
              s0 = peg$currPos;
              if (input.substr(peg$currPos, 7).toLowerCase() === peg$c113) {
                s1 = input.substr(peg$currPos, 7);
                peg$currPos += 7;
              } else {
                s1 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c114); }
              }
              if (s1 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c115();
              }
              s0 = s1;
              if (s0 === peg$FAILED) {
                s0 = peg$currPos;
                if (input.substr(peg$currPos, 3).toLowerCase() === peg$c116) {
                  s1 = input.substr(peg$currPos, 3);
                  peg$currPos += 3;
                } else {
                  s1 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c117); }
                }
                if (s1 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$c118();
                }
                s0 = s1;
                if (s0 === peg$FAILED) {
                  s0 = peg$currPos;
                  if (input.substr(peg$currPos, 3).toLowerCase() === peg$c119) {
                    s1 = input.substr(peg$currPos, 3);
                    peg$currPos += 3;
                  } else {
                    s1 = peg$FAILED;
                    if (peg$silentFails === 0) { peg$fail(peg$c120); }
                  }
                  if (s1 !== peg$FAILED) {
                    peg$savedPos = s0;
                    s1 = peg$c121();
                  }
                  s0 = s1;
                  if (s0 === peg$FAILED) {
                    s0 = peg$currPos;
                    if (input.substr(peg$currPos, 5).toLowerCase() === peg$c122) {
                      s1 = input.substr(peg$currPos, 5);
                      peg$currPos += 5;
                    } else {
                      s1 = peg$FAILED;
                      if (peg$silentFails === 0) { peg$fail(peg$c123); }
                    }
                    if (s1 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s1 = peg$c124();
                    }
                    s0 = s1;
                    if (s0 === peg$FAILED) {
                      s0 = peg$currPos;
                      if (input.substr(peg$currPos, 4).toLowerCase() === peg$c125) {
                        s1 = input.substr(peg$currPos, 4);
                        peg$currPos += 4;
                      } else {
                        s1 = peg$FAILED;
                        if (peg$silentFails === 0) { peg$fail(peg$c126); }
                      }
                      if (s1 !== peg$FAILED) {
                        peg$savedPos = s0;
                        s1 = peg$c127();
                      }
                      s0 = s1;
                      if (s0 === peg$FAILED) {
                        s0 = peg$currPos;
                        if (input.substr(peg$currPos, 13).toLowerCase() === peg$c128) {
                          s1 = input.substr(peg$currPos, 13);
                          peg$currPos += 13;
                        } else {
                          s1 = peg$FAILED;
                          if (peg$silentFails === 0) { peg$fail(peg$c129); }
                        }
                        if (s1 !== peg$FAILED) {
                          peg$savedPos = s0;
                          s1 = peg$c130();
                        }
                        s0 = s1;
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    return s0;
  }

  function peg$parsepaddedFieldExpr() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    s1 = peg$parse_();
    if (s1 === peg$FAILED) {
      s1 = null;
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parsefieldExpr();
      if (s2 !== peg$FAILED) {
        s3 = peg$parse_();
        if (s3 === peg$FAILED) {
          s3 = null;
        }
        if (s3 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c131(s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsecountReducer() {
    var s0, s1, s2, s3, s4, s5, s6;

    s0 = peg$currPos;
    s1 = peg$parsecountOp();
    if (s1 !== peg$FAILED) {
      s2 = peg$parse_();
      if (s2 === peg$FAILED) {
        s2 = null;
      }
      if (s2 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 40) {
          s3 = peg$c19;
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c20); }
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parsepaddedFieldExpr();
          if (s4 === peg$FAILED) {
            s4 = null;
          }
          if (s4 !== peg$FAILED) {
            s5 = peg$parse_();
            if (s5 === peg$FAILED) {
              s5 = null;
            }
            if (s5 !== peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 41) {
                s6 = peg$c21;
                peg$currPos++;
              } else {
                s6 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c22); }
              }
              if (s6 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c132(s1, s4);
                s0 = s1;
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsefieldReducer() {
    var s0, s1, s2, s3, s4, s5, s6, s7;

    s0 = peg$currPos;
    s1 = peg$parsefieldReducerOp();
    if (s1 !== peg$FAILED) {
      s2 = peg$parse_();
      if (s2 === peg$FAILED) {
        s2 = null;
      }
      if (s2 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 40) {
          s3 = peg$c19;
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c20); }
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parse_();
          if (s4 === peg$FAILED) {
            s4 = null;
          }
          if (s4 !== peg$FAILED) {
            s5 = peg$parsefieldExpr();
            if (s5 !== peg$FAILED) {
              s6 = peg$parse_();
              if (s6 === peg$FAILED) {
                s6 = null;
              }
              if (s6 !== peg$FAILED) {
                if (input.charCodeAt(peg$currPos) === 41) {
                  s7 = peg$c21;
                  peg$currPos++;
                } else {
                  s7 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c22); }
                }
                if (s7 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$c133(s1, s5);
                  s0 = s1;
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsegroupByProc() {
    var s0, s1, s2, s3, s4, s5;

    s0 = peg$currPos;
    s1 = peg$currPos;
    s2 = peg$parseeveryDur();
    if (s2 !== peg$FAILED) {
      s3 = peg$parse_();
      if (s3 !== peg$FAILED) {
        s2 = [s2, s3];
        s1 = s2;
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
    } else {
      peg$currPos = s1;
      s1 = peg$FAILED;
    }
    if (s1 === peg$FAILED) {
      s1 = null;
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parsereducerList();
      if (s2 !== peg$FAILED) {
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          s5 = peg$parsegroupByKeys();
          if (s5 !== peg$FAILED) {
            s4 = [s4, s5];
            s3 = s4;
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        if (s3 === peg$FAILED) {
          s3 = null;
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parseprocLimitArg();
          if (s4 === peg$FAILED) {
            s4 = null;
          }
          if (s4 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c134(s1, s2, s3, s4);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsereducerExpr() {
    var s0, s1, s2, s3, s4, s5;

    s0 = peg$currPos;
    s1 = peg$parsefieldName();
    if (s1 !== peg$FAILED) {
      s2 = peg$parse_();
      if (s2 === peg$FAILED) {
        s2 = null;
      }
      if (s2 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 61) {
          s3 = peg$c135;
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c136); }
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parse_();
          if (s4 === peg$FAILED) {
            s4 = null;
          }
          if (s4 !== peg$FAILED) {
            s5 = peg$parsereducer();
            if (s5 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c137(s1, s5);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$parsereducer();
    }

    return s0;
  }

  function peg$parsereducer() {
    var s0;

    s0 = peg$parsecountReducer();
    if (s0 === peg$FAILED) {
      s0 = peg$parsefieldReducer();
    }

    return s0;
  }

  function peg$parsereducerList() {
    var s0, s1, s2, s3, s4, s5, s6, s7;

    s0 = peg$currPos;
    s1 = peg$parsereducerExpr();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$currPos;
      s4 = peg$parse_();
      if (s4 === peg$FAILED) {
        s4 = null;
      }
      if (s4 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 44) {
          s5 = peg$c60;
          peg$currPos++;
        } else {
          s5 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c61); }
        }
        if (s5 !== peg$FAILED) {
          s6 = peg$parse_();
          if (s6 === peg$FAILED) {
            s6 = null;
          }
          if (s6 !== peg$FAILED) {
            s7 = peg$parsereducerExpr();
            if (s7 !== peg$FAILED) {
              s4 = [s4, s5, s6, s7];
              s3 = s4;
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      } else {
        peg$currPos = s3;
        s3 = peg$FAILED;
      }
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 === peg$FAILED) {
          s4 = null;
        }
        if (s4 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 44) {
            s5 = peg$c60;
            peg$currPos++;
          } else {
            s5 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c61); }
          }
          if (s5 !== peg$FAILED) {
            s6 = peg$parse_();
            if (s6 === peg$FAILED) {
              s6 = null;
            }
            if (s6 !== peg$FAILED) {
              s7 = peg$parsereducerExpr();
              if (s7 !== peg$FAILED) {
                s4 = [s4, s5, s6, s7];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c138(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsesimpleProc() {
    var s0;

    s0 = peg$parsesort();
    if (s0 === peg$FAILED) {
      s0 = peg$parsetop();
      if (s0 === peg$FAILED) {
        s0 = peg$parsecut();
        if (s0 === peg$FAILED) {
          s0 = peg$parsehead();
          if (s0 === peg$FAILED) {
            s0 = peg$parsetail();
            if (s0 === peg$FAILED) {
              s0 = peg$parsefilter();
              if (s0 === peg$FAILED) {
                s0 = peg$parseuniq();
                if (s0 === peg$FAILED) {
                  s0 = peg$parseput();
                  if (s0 === peg$FAILED) {
                    s0 = peg$parserename();
                  }
                }
              }
            }
          }
        }
      }
    }

    return s0;
  }

  function peg$parsesort() {
    var s0, s1, s2, s3, s4, s5;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 4).toLowerCase() === peg$c139) {
      s1 = input.substr(peg$currPos, 4);
      peg$currPos += 4;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c140); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parsesortArgs();
      if (s2 !== peg$FAILED) {
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          s5 = peg$parsefieldExprList();
          if (s5 !== peg$FAILED) {
            peg$savedPos = s3;
            s4 = peg$c141(s2, s5);
            s3 = s4;
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        if (s3 === peg$FAILED) {
          s3 = null;
        }
        if (s3 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c142(s2, s3);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsesortArgs() {
    var s0, s1, s2, s3, s4;

    s0 = peg$currPos;
    s1 = [];
    s2 = peg$currPos;
    s3 = peg$parse_();
    if (s3 !== peg$FAILED) {
      s4 = peg$parsesortArg();
      if (s4 !== peg$FAILED) {
        peg$savedPos = s2;
        s3 = peg$c143(s4);
        s2 = s3;
      } else {
        peg$currPos = s2;
        s2 = peg$FAILED;
      }
    } else {
      peg$currPos = s2;
      s2 = peg$FAILED;
    }
    while (s2 !== peg$FAILED) {
      s1.push(s2);
      s2 = peg$currPos;
      s3 = peg$parse_();
      if (s3 !== peg$FAILED) {
        s4 = peg$parsesortArg();
        if (s4 !== peg$FAILED) {
          peg$savedPos = s2;
          s3 = peg$c143(s4);
          s2 = s3;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
      } else {
        peg$currPos = s2;
        s2 = peg$FAILED;
      }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c144(s1);
    }
    s0 = s1;

    return s0;
  }

  function peg$parsesortArg() {
    var s0, s1, s2, s3, s4;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 2) === peg$c145) {
      s1 = peg$c145;
      peg$currPos += 2;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c146); }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c147();
    }
    s0 = s1;
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      if (input.substr(peg$currPos, 6) === peg$c148) {
        s1 = peg$c148;
        peg$currPos += 6;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c149); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$parse_();
        if (s2 !== peg$FAILED) {
          s3 = peg$currPos;
          if (input.substr(peg$currPos, 5) === peg$c122) {
            s4 = peg$c122;
            peg$currPos += 5;
          } else {
            s4 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c150); }
          }
          if (s4 === peg$FAILED) {
            if (input.substr(peg$currPos, 4) === peg$c125) {
              s4 = peg$c125;
              peg$currPos += 4;
            } else {
              s4 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c151); }
            }
          }
          if (s4 !== peg$FAILED) {
            peg$savedPos = s3;
            s4 = peg$c70();
          }
          s3 = s4;
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c152(s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    }

    return s0;
  }

  function peg$parsetop() {
    var s0, s1, s2, s3, s4, s5, s6;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 3).toLowerCase() === peg$c153) {
      s1 = input.substr(peg$currPos, 3);
      peg$currPos += 3;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c154); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$currPos;
      s3 = peg$parse_();
      if (s3 !== peg$FAILED) {
        s4 = peg$parseunsignedInteger();
        if (s4 !== peg$FAILED) {
          peg$savedPos = s2;
          s3 = peg$c155(s4);
          s2 = s3;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
      } else {
        peg$currPos = s2;
        s2 = peg$FAILED;
      }
      if (s2 === peg$FAILED) {
        s2 = null;
      }
      if (s2 !== peg$FAILED) {
        s3 = peg$currPos;
        s4 = peg$parse_();
        if (s4 !== peg$FAILED) {
          if (input.substr(peg$currPos, 6) === peg$c156) {
            s5 = peg$c156;
            peg$currPos += 6;
          } else {
            s5 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c157); }
          }
          if (s5 !== peg$FAILED) {
            s4 = [s4, s5];
            s3 = s4;
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        if (s3 === peg$FAILED) {
          s3 = null;
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$currPos;
          s5 = peg$parse_();
          if (s5 !== peg$FAILED) {
            s6 = peg$parsefieldExprList();
            if (s6 !== peg$FAILED) {
              peg$savedPos = s4;
              s5 = peg$c158(s2, s3, s6);
              s4 = s5;
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          } else {
            peg$currPos = s4;
            s4 = peg$FAILED;
          }
          if (s4 === peg$FAILED) {
            s4 = null;
          }
          if (s4 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c159(s2, s3, s4);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseprocLimitArg() {
    var s0, s1, s2, s3, s4;

    s0 = peg$currPos;
    s1 = peg$parse_();
    if (s1 !== peg$FAILED) {
      if (input.substr(peg$currPos, 6) === peg$c160) {
        s2 = peg$c160;
        peg$currPos += 6;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c161); }
      }
      if (s2 !== peg$FAILED) {
        s3 = peg$parse_();
        if (s3 !== peg$FAILED) {
          s4 = peg$parseunsignedInteger();
          if (s4 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c162(s4);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsecutArgs() {
    var s0, s1, s2, s3, s4;

    s0 = peg$currPos;
    s1 = [];
    s2 = peg$currPos;
    s3 = peg$parse_();
    if (s3 !== peg$FAILED) {
      if (input.substr(peg$currPos, 2) === peg$c163) {
        s4 = peg$c163;
        peg$currPos += 2;
      } else {
        s4 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c164); }
      }
      if (s4 !== peg$FAILED) {
        peg$savedPos = s2;
        s3 = peg$c165();
        s2 = s3;
      } else {
        peg$currPos = s2;
        s2 = peg$FAILED;
      }
    } else {
      peg$currPos = s2;
      s2 = peg$FAILED;
    }
    while (s2 !== peg$FAILED) {
      s1.push(s2);
      s2 = peg$currPos;
      s3 = peg$parse_();
      if (s3 !== peg$FAILED) {
        if (input.substr(peg$currPos, 2) === peg$c163) {
          s4 = peg$c163;
          peg$currPos += 2;
        } else {
          s4 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c164); }
        }
        if (s4 !== peg$FAILED) {
          peg$savedPos = s2;
          s3 = peg$c165();
          s2 = s3;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
      } else {
        peg$currPos = s2;
        s2 = peg$FAILED;
      }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c166(s1);
    }
    s0 = s1;

    return s0;
  }

  function peg$parsecutAssignment() {
    var s0, s1;

    s0 = peg$parseFieldAssignment();
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      s1 = peg$parsefieldRefDotOnly();
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c167(s1);
      }
      s0 = s1;
    }

    return s0;
  }

  function peg$parsecut() {
    var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 3).toLowerCase() === peg$c168) {
      s1 = input.substr(peg$currPos, 3);
      peg$currPos += 3;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c169); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parsecutArgs();
      if (s2 !== peg$FAILED) {
        s3 = peg$parse_();
        if (s3 !== peg$FAILED) {
          s4 = peg$parsecutAssignment();
          if (s4 !== peg$FAILED) {
            s5 = [];
            s6 = peg$currPos;
            s7 = peg$parse__();
            if (s7 !== peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 44) {
                s8 = peg$c60;
                peg$currPos++;
              } else {
                s8 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c61); }
              }
              if (s8 !== peg$FAILED) {
                s9 = peg$parse__();
                if (s9 !== peg$FAILED) {
                  s10 = peg$parsecutAssignment();
                  if (s10 !== peg$FAILED) {
                    peg$savedPos = s6;
                    s7 = peg$c170(s2, s4, s10);
                    s6 = s7;
                  } else {
                    peg$currPos = s6;
                    s6 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s6;
                  s6 = peg$FAILED;
                }
              } else {
                peg$currPos = s6;
                s6 = peg$FAILED;
              }
            } else {
              peg$currPos = s6;
              s6 = peg$FAILED;
            }
            while (s6 !== peg$FAILED) {
              s5.push(s6);
              s6 = peg$currPos;
              s7 = peg$parse__();
              if (s7 !== peg$FAILED) {
                if (input.charCodeAt(peg$currPos) === 44) {
                  s8 = peg$c60;
                  peg$currPos++;
                } else {
                  s8 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c61); }
                }
                if (s8 !== peg$FAILED) {
                  s9 = peg$parse__();
                  if (s9 !== peg$FAILED) {
                    s10 = peg$parsecutAssignment();
                    if (s10 !== peg$FAILED) {
                      peg$savedPos = s6;
                      s7 = peg$c170(s2, s4, s10);
                      s6 = s7;
                    } else {
                      peg$currPos = s6;
                      s6 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s6;
                    s6 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s6;
                  s6 = peg$FAILED;
                }
              } else {
                peg$currPos = s6;
                s6 = peg$FAILED;
              }
            }
            if (s5 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c171(s2, s4, s5);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsehead() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 4).toLowerCase() === peg$c172) {
      s1 = input.substr(peg$currPos, 4);
      peg$currPos += 4;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c173); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parse_();
      if (s2 !== peg$FAILED) {
        s3 = peg$parseunsignedInteger();
        if (s3 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c174(s3);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      if (input.substr(peg$currPos, 4).toLowerCase() === peg$c172) {
        s1 = input.substr(peg$currPos, 4);
        peg$currPos += 4;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c173); }
      }
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c175();
      }
      s0 = s1;
    }

    return s0;
  }

  function peg$parsetail() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 4).toLowerCase() === peg$c176) {
      s1 = input.substr(peg$currPos, 4);
      peg$currPos += 4;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c177); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parse_();
      if (s2 !== peg$FAILED) {
        s3 = peg$parseunsignedInteger();
        if (s3 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c178(s3);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      if (input.substr(peg$currPos, 4).toLowerCase() === peg$c176) {
        s1 = input.substr(peg$currPos, 4);
        peg$currPos += 4;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c177); }
      }
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c179();
      }
      s0 = s1;
    }

    return s0;
  }

  function peg$parsefilter() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 6).toLowerCase() === peg$c180) {
      s1 = input.substr(peg$currPos, 6);
      peg$currPos += 6;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c181); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parse_();
      if (s2 !== peg$FAILED) {
        s3 = peg$parsesearchExpr();
        if (s3 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c8(s3);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseuniq() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 4).toLowerCase() === peg$c182) {
      s1 = input.substr(peg$currPos, 4);
      peg$currPos += 4;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c183); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parse_();
      if (s2 !== peg$FAILED) {
        if (input.substr(peg$currPos, 2) === peg$c163) {
          s3 = peg$c163;
          peg$currPos += 2;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c164); }
        }
        if (s3 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c184();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      if (input.substr(peg$currPos, 4).toLowerCase() === peg$c182) {
        s1 = input.substr(peg$currPos, 4);
        peg$currPos += 4;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c183); }
      }
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c185();
      }
      s0 = s1;
    }

    return s0;
  }

  function peg$parseput() {
    var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 3).toLowerCase() === peg$c186) {
      s1 = input.substr(peg$currPos, 3);
      peg$currPos += 3;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c187); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parse_();
      if (s2 !== peg$FAILED) {
        s3 = peg$parseExpressionAssignment();
        if (s3 !== peg$FAILED) {
          s4 = [];
          s5 = peg$currPos;
          s6 = peg$parse__();
          if (s6 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 44) {
              s7 = peg$c60;
              peg$currPos++;
            } else {
              s7 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c61); }
            }
            if (s7 !== peg$FAILED) {
              s8 = peg$parse__();
              if (s8 !== peg$FAILED) {
                s9 = peg$parseExpressionAssignment();
                if (s9 !== peg$FAILED) {
                  peg$savedPos = s5;
                  s6 = peg$c62(s3, s9);
                  s5 = s6;
                } else {
                  peg$currPos = s5;
                  s5 = peg$FAILED;
                }
              } else {
                peg$currPos = s5;
                s5 = peg$FAILED;
              }
            } else {
              peg$currPos = s5;
              s5 = peg$FAILED;
            }
          } else {
            peg$currPos = s5;
            s5 = peg$FAILED;
          }
          while (s5 !== peg$FAILED) {
            s4.push(s5);
            s5 = peg$currPos;
            s6 = peg$parse__();
            if (s6 !== peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 44) {
                s7 = peg$c60;
                peg$currPos++;
              } else {
                s7 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c61); }
              }
              if (s7 !== peg$FAILED) {
                s8 = peg$parse__();
                if (s8 !== peg$FAILED) {
                  s9 = peg$parseExpressionAssignment();
                  if (s9 !== peg$FAILED) {
                    peg$savedPos = s5;
                    s6 = peg$c62(s3, s9);
                    s5 = s6;
                  } else {
                    peg$currPos = s5;
                    s5 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s5;
                  s5 = peg$FAILED;
                }
              } else {
                peg$currPos = s5;
                s5 = peg$FAILED;
              }
            } else {
              peg$currPos = s5;
              s5 = peg$FAILED;
            }
          }
          if (s4 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c188(s3, s4);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parserename() {
    var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 6).toLowerCase() === peg$c189) {
      s1 = input.substr(peg$currPos, 6);
      peg$currPos += 6;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c190); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parse_();
      if (s2 !== peg$FAILED) {
        s3 = peg$parseFieldAssignment();
        if (s3 !== peg$FAILED) {
          s4 = [];
          s5 = peg$currPos;
          s6 = peg$parse__();
          if (s6 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 44) {
              s7 = peg$c60;
              peg$currPos++;
            } else {
              s7 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c61); }
            }
            if (s7 !== peg$FAILED) {
              s8 = peg$parse__();
              if (s8 !== peg$FAILED) {
                s9 = peg$parseFieldAssignment();
                if (s9 !== peg$FAILED) {
                  peg$savedPos = s5;
                  s6 = peg$c62(s3, s9);
                  s5 = s6;
                } else {
                  peg$currPos = s5;
                  s5 = peg$FAILED;
                }
              } else {
                peg$currPos = s5;
                s5 = peg$FAILED;
              }
            } else {
              peg$currPos = s5;
              s5 = peg$FAILED;
            }
          } else {
            peg$currPos = s5;
            s5 = peg$FAILED;
          }
          while (s5 !== peg$FAILED) {
            s4.push(s5);
            s5 = peg$currPos;
            s6 = peg$parse__();
            if (s6 !== peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 44) {
                s7 = peg$c60;
                peg$currPos++;
              } else {
                s7 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c61); }
              }
              if (s7 !== peg$FAILED) {
                s8 = peg$parse__();
                if (s8 !== peg$FAILED) {
                  s9 = peg$parseFieldAssignment();
                  if (s9 !== peg$FAILED) {
                    peg$savedPos = s5;
                    s6 = peg$c62(s3, s9);
                    s5 = s6;
                  } else {
                    peg$currPos = s5;
                    s5 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s5;
                  s5 = peg$FAILED;
                }
              } else {
                peg$currPos = s5;
                s5 = peg$FAILED;
              }
            } else {
              peg$currPos = s5;
              s5 = peg$FAILED;
            }
          }
          if (s4 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c191(s3, s4);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseExpressionAssignment() {
    var s0, s1, s2, s3, s4, s5;

    s0 = peg$currPos;
    s1 = peg$parsefieldName();
    if (s1 !== peg$FAILED) {
      s2 = peg$parse__();
      if (s2 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 61) {
          s3 = peg$c135;
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c136); }
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parse__();
          if (s4 !== peg$FAILED) {
            s5 = peg$parseConditionalExpression();
            if (s5 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c192(s1, s5);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseFieldAssignment() {
    var s0, s1, s2, s3, s4, s5;

    s0 = peg$currPos;
    s1 = peg$parsefieldRefDotOnly();
    if (s1 !== peg$FAILED) {
      s2 = peg$parse__();
      if (s2 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 61) {
          s3 = peg$c135;
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c136); }
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parse__();
          if (s4 !== peg$FAILED) {
            s5 = peg$parsefieldRefDotOnly();
            if (s5 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c193(s1, s5);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsePrimaryExpression() {
    var s0, s1, s2, s3, s4, s5;

    s0 = peg$parseStringLiteral();
    if (s0 === peg$FAILED) {
      s0 = peg$parseRegexpLiteral();
      if (s0 === peg$FAILED) {
        s0 = peg$parsePortLiteral();
        if (s0 === peg$FAILED) {
          s0 = peg$parseSubnetLiteral();
          if (s0 === peg$FAILED) {
            s0 = peg$parseAddressLiteral();
            if (s0 === peg$FAILED) {
              s0 = peg$parseFloatLiteral();
              if (s0 === peg$FAILED) {
                s0 = peg$parseIntegerLiteral();
                if (s0 === peg$FAILED) {
                  s0 = peg$parseBooleanLiteral();
                  if (s0 === peg$FAILED) {
                    s0 = peg$parseNullLiteral();
                    if (s0 === peg$FAILED) {
                      s0 = peg$parseFieldReference();
                      if (s0 === peg$FAILED) {
                        s0 = peg$currPos;
                        if (input.charCodeAt(peg$currPos) === 40) {
                          s1 = peg$c19;
                          peg$currPos++;
                        } else {
                          s1 = peg$FAILED;
                          if (peg$silentFails === 0) { peg$fail(peg$c20); }
                        }
                        if (s1 !== peg$FAILED) {
                          s2 = peg$parse__();
                          if (s2 !== peg$FAILED) {
                            s3 = peg$parseConditionalExpression();
                            if (s3 !== peg$FAILED) {
                              s4 = peg$parse__();
                              if (s4 !== peg$FAILED) {
                                if (input.charCodeAt(peg$currPos) === 41) {
                                  s5 = peg$c21;
                                  peg$currPos++;
                                } else {
                                  s5 = peg$FAILED;
                                  if (peg$silentFails === 0) { peg$fail(peg$c22); }
                                }
                                if (s5 !== peg$FAILED) {
                                  peg$savedPos = s0;
                                  s1 = peg$c23(s3);
                                  s0 = s1;
                                } else {
                                  peg$currPos = s0;
                                  s0 = peg$FAILED;
                                }
                              } else {
                                peg$currPos = s0;
                                s0 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s0;
                              s0 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    return s0;
  }

  function peg$parseFieldReference() {
    var s0, s1;

    s0 = peg$currPos;
    s1 = peg$parsefieldName();
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c194(s1);
    }
    s0 = s1;

    return s0;
  }

  function peg$parseExpression() {
    var s0;

    s0 = peg$parseConditionalExpression();

    return s0;
  }

  function peg$parseConditionalExpression() {
    var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9;

    s0 = peg$currPos;
    s1 = peg$parseLogicalORExpression();
    if (s1 !== peg$FAILED) {
      s2 = peg$parse__();
      if (s2 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 63) {
          s3 = peg$c195;
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c196); }
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parse__();
          if (s4 !== peg$FAILED) {
            s5 = peg$parseConditionalExpression();
            if (s5 !== peg$FAILED) {
              s6 = peg$parse__();
              if (s6 !== peg$FAILED) {
                if (input.charCodeAt(peg$currPos) === 58) {
                  s7 = peg$c197;
                  peg$currPos++;
                } else {
                  s7 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c198); }
                }
                if (s7 !== peg$FAILED) {
                  s8 = peg$parse__();
                  if (s8 !== peg$FAILED) {
                    s9 = peg$parseConditionalExpression();
                    if (s9 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s1 = peg$c199(s1, s5, s9);
                      s0 = s1;
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$parseLogicalORExpression();
    }

    return s0;
  }

  function peg$parseLogicalORExpression() {
    var s0, s1, s2, s3, s4, s5, s6, s7;

    s0 = peg$currPos;
    s1 = peg$parseLogicalANDExpression();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$currPos;
      s4 = peg$parse__();
      if (s4 !== peg$FAILED) {
        s5 = peg$parseorToken();
        if (s5 !== peg$FAILED) {
          s6 = peg$parse__();
          if (s6 !== peg$FAILED) {
            s7 = peg$parseLogicalANDExpression();
            if (s7 !== peg$FAILED) {
              peg$savedPos = s3;
              s4 = peg$c200(s1, s5, s7);
              s3 = s4;
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      } else {
        peg$currPos = s3;
        s3 = peg$FAILED;
      }
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$currPos;
        s4 = peg$parse__();
        if (s4 !== peg$FAILED) {
          s5 = peg$parseorToken();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse__();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseLogicalANDExpression();
              if (s7 !== peg$FAILED) {
                peg$savedPos = s3;
                s4 = peg$c200(s1, s5, s7);
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c201(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseLogicalANDExpression() {
    var s0, s1, s2, s3, s4, s5, s6, s7;

    s0 = peg$currPos;
    s1 = peg$parseEqualityCompareExpression();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$currPos;
      s4 = peg$parse__();
      if (s4 !== peg$FAILED) {
        s5 = peg$parseandToken();
        if (s5 !== peg$FAILED) {
          s6 = peg$parse__();
          if (s6 !== peg$FAILED) {
            s7 = peg$parseEqualityCompareExpression();
            if (s7 !== peg$FAILED) {
              peg$savedPos = s3;
              s4 = peg$c200(s1, s5, s7);
              s3 = s4;
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      } else {
        peg$currPos = s3;
        s3 = peg$FAILED;
      }
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$currPos;
        s4 = peg$parse__();
        if (s4 !== peg$FAILED) {
          s5 = peg$parseandToken();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse__();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseEqualityCompareExpression();
              if (s7 !== peg$FAILED) {
                peg$savedPos = s3;
                s4 = peg$c200(s1, s5, s7);
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c201(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseEqualityCompareExpression() {
    var s0, s1, s2, s3, s4, s5, s6, s7;

    s0 = peg$currPos;
    s1 = peg$parseRelativeExpression();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$currPos;
      s4 = peg$parse__();
      if (s4 !== peg$FAILED) {
        s5 = peg$parseEqualityComparator();
        if (s5 !== peg$FAILED) {
          s6 = peg$parse__();
          if (s6 !== peg$FAILED) {
            s7 = peg$parseRelativeExpression();
            if (s7 !== peg$FAILED) {
              peg$savedPos = s3;
              s4 = peg$c202(s1, s5, s7);
              s3 = s4;
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      } else {
        peg$currPos = s3;
        s3 = peg$FAILED;
      }
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$currPos;
        s4 = peg$parse__();
        if (s4 !== peg$FAILED) {
          s5 = peg$parseEqualityComparator();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse__();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseRelativeExpression();
              if (s7 !== peg$FAILED) {
                peg$savedPos = s3;
                s4 = peg$c202(s1, s5, s7);
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c201(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseEqualityOperator() {
    var s0, s1;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 2) === peg$c203) {
      s1 = peg$c203;
      peg$currPos += 2;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c204); }
    }
    if (s1 === peg$FAILED) {
      if (input.substr(peg$currPos, 2) === peg$c205) {
        s1 = peg$c205;
        peg$currPos += 2;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c206); }
      }
      if (s1 === peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 61) {
          s1 = peg$c135;
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c136); }
        }
        if (s1 === peg$FAILED) {
          if (input.substr(peg$currPos, 2) === peg$c207) {
            s1 = peg$c207;
            peg$currPos += 2;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c208); }
          }
        }
      }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c70();
    }
    s0 = s1;

    return s0;
  }

  function peg$parseEqualityComparator() {
    var s0, s1;

    s0 = peg$parseEqualityOperator();
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      if (input.substr(peg$currPos, 2) === peg$c73) {
        s1 = peg$c73;
        peg$currPos += 2;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c209); }
      }
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c70();
      }
      s0 = s1;
    }

    return s0;
  }

  function peg$parseRelativeExpression() {
    var s0, s1, s2, s3, s4, s5, s6, s7;

    s0 = peg$currPos;
    s1 = peg$parseAdditiveExpression();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$currPos;
      s4 = peg$parse__();
      if (s4 !== peg$FAILED) {
        s5 = peg$parseRelativeOperator();
        if (s5 !== peg$FAILED) {
          s6 = peg$parse__();
          if (s6 !== peg$FAILED) {
            s7 = peg$parseAdditiveExpression();
            if (s7 !== peg$FAILED) {
              peg$savedPos = s3;
              s4 = peg$c200(s1, s5, s7);
              s3 = s4;
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      } else {
        peg$currPos = s3;
        s3 = peg$FAILED;
      }
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$currPos;
        s4 = peg$parse__();
        if (s4 !== peg$FAILED) {
          s5 = peg$parseRelativeOperator();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse__();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseAdditiveExpression();
              if (s7 !== peg$FAILED) {
                peg$savedPos = s3;
                s4 = peg$c200(s1, s5, s7);
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c201(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseRelativeOperator() {
    var s0, s1;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 2) === peg$c210) {
      s1 = peg$c210;
      peg$currPos += 2;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c211); }
    }
    if (s1 === peg$FAILED) {
      if (input.charCodeAt(peg$currPos) === 60) {
        s1 = peg$c212;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c213); }
      }
      if (s1 === peg$FAILED) {
        if (input.substr(peg$currPos, 2) === peg$c214) {
          s1 = peg$c214;
          peg$currPos += 2;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c215); }
        }
        if (s1 === peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 62) {
            s1 = peg$c216;
            peg$currPos++;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c217); }
          }
        }
      }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c70();
    }
    s0 = s1;

    return s0;
  }

  function peg$parseAdditiveExpression() {
    var s0, s1, s2, s3, s4, s5, s6, s7;

    s0 = peg$currPos;
    s1 = peg$parseMultiplicativeExpression();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$currPos;
      s4 = peg$parse__();
      if (s4 !== peg$FAILED) {
        s5 = peg$parseAdditiveOperator();
        if (s5 !== peg$FAILED) {
          s6 = peg$parse__();
          if (s6 !== peg$FAILED) {
            s7 = peg$parseMultiplicativeExpression();
            if (s7 !== peg$FAILED) {
              peg$savedPos = s3;
              s4 = peg$c200(s1, s5, s7);
              s3 = s4;
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      } else {
        peg$currPos = s3;
        s3 = peg$FAILED;
      }
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$currPos;
        s4 = peg$parse__();
        if (s4 !== peg$FAILED) {
          s5 = peg$parseAdditiveOperator();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse__();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseMultiplicativeExpression();
              if (s7 !== peg$FAILED) {
                peg$savedPos = s3;
                s4 = peg$c200(s1, s5, s7);
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c201(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseAdditiveOperator() {
    var s0, s1;

    s0 = peg$currPos;
    if (input.charCodeAt(peg$currPos) === 43) {
      s1 = peg$c218;
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c219); }
    }
    if (s1 === peg$FAILED) {
      if (input.charCodeAt(peg$currPos) === 45) {
        s1 = peg$c16;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c17); }
      }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c70();
    }
    s0 = s1;

    return s0;
  }

  function peg$parseMultiplicativeExpression() {
    var s0, s1, s2, s3, s4, s5, s6, s7;

    s0 = peg$currPos;
    s1 = peg$parseNotExpression();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$currPos;
      s4 = peg$parse__();
      if (s4 !== peg$FAILED) {
        s5 = peg$parseMultiplicativeOperator();
        if (s5 !== peg$FAILED) {
          s6 = peg$parse__();
          if (s6 !== peg$FAILED) {
            s7 = peg$parseNotExpression();
            if (s7 !== peg$FAILED) {
              peg$savedPos = s3;
              s4 = peg$c200(s1, s5, s7);
              s3 = s4;
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      } else {
        peg$currPos = s3;
        s3 = peg$FAILED;
      }
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$currPos;
        s4 = peg$parse__();
        if (s4 !== peg$FAILED) {
          s5 = peg$parseMultiplicativeOperator();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse__();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseNotExpression();
              if (s7 !== peg$FAILED) {
                peg$savedPos = s3;
                s4 = peg$c200(s1, s5, s7);
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c201(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseMultiplicativeOperator() {
    var s0, s1;

    s0 = peg$currPos;
    if (input.charCodeAt(peg$currPos) === 42) {
      s1 = peg$c24;
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c25); }
    }
    if (s1 === peg$FAILED) {
      if (input.charCodeAt(peg$currPos) === 47) {
        s1 = peg$c220;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c221); }
      }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c70();
    }
    s0 = s1;

    return s0;
  }

  function peg$parseNotExpression() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    if (input.charCodeAt(peg$currPos) === 33) {
      s1 = peg$c13;
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c14); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parse__();
      if (s2 !== peg$FAILED) {
        s3 = peg$parseNotExpression();
        if (s3 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c222(s3);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$parseCastExpression();
    }

    return s0;
  }

  function peg$parseCastExpression() {
    var s0, s1, s2, s3, s4, s5, s6;

    s0 = peg$currPos;
    s1 = peg$parseCallExpression();
    if (s1 !== peg$FAILED) {
      s2 = peg$currPos;
      s3 = peg$parse__();
      if (s3 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 58) {
          s4 = peg$c197;
          peg$currPos++;
        } else {
          s4 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c198); }
        }
        if (s4 !== peg$FAILED) {
          s5 = peg$parse__();
          if (s5 !== peg$FAILED) {
            s6 = peg$parseZngType();
            if (s6 !== peg$FAILED) {
              peg$savedPos = s2;
              s3 = peg$c223(s1, s6);
              s2 = s3;
            } else {
              peg$currPos = s2;
              s2 = peg$FAILED;
            }
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
      } else {
        peg$currPos = s2;
        s2 = peg$FAILED;
      }
      if (s2 === peg$FAILED) {
        s2 = null;
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c224(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseZngType() {
    var s0, s1;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 4) === peg$c225) {
      s1 = peg$c225;
      peg$currPos += 4;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c226); }
    }
    if (s1 === peg$FAILED) {
      if (input.substr(peg$currPos, 4) === peg$c227) {
        s1 = peg$c227;
        peg$currPos += 4;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c228); }
      }
      if (s1 === peg$FAILED) {
        if (input.substr(peg$currPos, 5) === peg$c229) {
          s1 = peg$c229;
          peg$currPos += 5;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c230); }
        }
        if (s1 === peg$FAILED) {
          if (input.substr(peg$currPos, 6) === peg$c231) {
            s1 = peg$c231;
            peg$currPos += 6;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c232); }
          }
          if (s1 === peg$FAILED) {
            if (input.substr(peg$currPos, 5) === peg$c233) {
              s1 = peg$c233;
              peg$currPos += 5;
            } else {
              s1 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c234); }
            }
            if (s1 === peg$FAILED) {
              if (input.substr(peg$currPos, 6) === peg$c235) {
                s1 = peg$c235;
                peg$currPos += 6;
              } else {
                s1 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c236); }
              }
              if (s1 === peg$FAILED) {
                if (input.substr(peg$currPos, 5) === peg$c237) {
                  s1 = peg$c237;
                  peg$currPos += 5;
                } else {
                  s1 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c238); }
                }
                if (s1 === peg$FAILED) {
                  if (input.substr(peg$currPos, 6) === peg$c239) {
                    s1 = peg$c239;
                    peg$currPos += 6;
                  } else {
                    s1 = peg$FAILED;
                    if (peg$silentFails === 0) { peg$fail(peg$c240); }
                  }
                  if (s1 === peg$FAILED) {
                    if (input.substr(peg$currPos, 7) === peg$c241) {
                      s1 = peg$c241;
                      peg$currPos += 7;
                    } else {
                      s1 = peg$FAILED;
                      if (peg$silentFails === 0) { peg$fail(peg$c242); }
                    }
                    if (s1 === peg$FAILED) {
                      if (input.substr(peg$currPos, 6) === peg$c243) {
                        s1 = peg$c243;
                        peg$currPos += 6;
                      } else {
                        s1 = peg$FAILED;
                        if (peg$silentFails === 0) { peg$fail(peg$c244); }
                      }
                      if (s1 === peg$FAILED) {
                        if (input.substr(peg$currPos, 7) === peg$c245) {
                          s1 = peg$c245;
                          peg$currPos += 7;
                        } else {
                          s1 = peg$FAILED;
                          if (peg$silentFails === 0) { peg$fail(peg$c246); }
                        }
                        if (s1 === peg$FAILED) {
                          if (input.substr(peg$currPos, 2) === peg$c247) {
                            s1 = peg$c247;
                            peg$currPos += 2;
                          } else {
                            s1 = peg$FAILED;
                            if (peg$silentFails === 0) { peg$fail(peg$c248); }
                          }
                          if (s1 === peg$FAILED) {
                            if (input.substr(peg$currPos, 3) === peg$c249) {
                              s1 = peg$c249;
                              peg$currPos += 3;
                            } else {
                              s1 = peg$FAILED;
                              if (peg$silentFails === 0) { peg$fail(peg$c250); }
                            }
                            if (s1 === peg$FAILED) {
                              if (input.substr(peg$currPos, 4) === peg$c251) {
                                s1 = peg$c251;
                                peg$currPos += 4;
                              } else {
                                s1 = peg$FAILED;
                                if (peg$silentFails === 0) { peg$fail(peg$c252); }
                              }
                              if (s1 === peg$FAILED) {
                                if (input.substr(peg$currPos, 8) === peg$c253) {
                                  s1 = peg$c253;
                                  peg$currPos += 8;
                                } else {
                                  s1 = peg$FAILED;
                                  if (peg$silentFails === 0) { peg$fail(peg$c254); }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c70();
    }
    s0 = s1;

    return s0;
  }

  function peg$parseCallExpression() {
    var s0, s1, s2, s3, s4, s5;

    s0 = peg$currPos;
    s1 = peg$parseFunctionName();
    if (s1 !== peg$FAILED) {
      s2 = peg$parse__();
      if (s2 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 40) {
          s3 = peg$c19;
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c20); }
        }
        if (s3 !== peg$FAILED) {
          s4 = peg$parseArgumentList();
          if (s4 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 41) {
              s5 = peg$c21;
              peg$currPos++;
            } else {
              s5 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c22); }
            }
            if (s5 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c255(s1, s4);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$parseDereferenceExpression();
    }

    return s0;
  }

  function peg$parseFunctionName() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    s1 = peg$parseFunctionNameStart();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$parseFunctionNameRest();
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$parseFunctionNameRest();
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c70();
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseFunctionNameStart() {
    var s0;

    if (peg$c256.test(input.charAt(peg$currPos))) {
      s0 = input.charAt(peg$currPos);
      peg$currPos++;
    } else {
      s0 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c257); }
    }

    return s0;
  }

  function peg$parseFunctionNameRest() {
    var s0;

    s0 = peg$parseFunctionNameStart();
    if (s0 === peg$FAILED) {
      if (peg$c258.test(input.charAt(peg$currPos))) {
        s0 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c259); }
      }
    }

    return s0;
  }

  function peg$parseArgumentList() {
    var s0, s1, s2, s3, s4, s5, s6, s7;

    s0 = peg$currPos;
    s1 = peg$parseConditionalExpression();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$currPos;
      s4 = peg$parse__();
      if (s4 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 44) {
          s5 = peg$c60;
          peg$currPos++;
        } else {
          s5 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c61); }
        }
        if (s5 !== peg$FAILED) {
          s6 = peg$parse__();
          if (s6 !== peg$FAILED) {
            s7 = peg$parseConditionalExpression();
            if (s7 !== peg$FAILED) {
              peg$savedPos = s3;
              s4 = peg$c260(s1, s7);
              s3 = s4;
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      } else {
        peg$currPos = s3;
        s3 = peg$FAILED;
      }
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$currPos;
        s4 = peg$parse__();
        if (s4 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 44) {
            s5 = peg$c60;
            peg$currPos++;
          } else {
            s5 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c61); }
          }
          if (s5 !== peg$FAILED) {
            s6 = peg$parse__();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseConditionalExpression();
              if (s7 !== peg$FAILED) {
                peg$savedPos = s3;
                s4 = peg$c260(s1, s7);
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c261(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      s1 = peg$parse__();
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c262();
      }
      s0 = s1;
    }

    return s0;
  }

  function peg$parseDereferenceExpression() {
    var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9;

    s0 = peg$currPos;
    s1 = peg$parsePrimaryExpression();
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$currPos;
      s4 = peg$parse__();
      if (s4 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 91) {
          s5 = peg$c84;
          peg$currPos++;
        } else {
          s5 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c85); }
        }
        if (s5 !== peg$FAILED) {
          s6 = peg$parse__();
          if (s6 !== peg$FAILED) {
            s7 = peg$parseConditionalExpression();
            if (s7 !== peg$FAILED) {
              s8 = peg$parse__();
              if (s8 !== peg$FAILED) {
                if (input.charCodeAt(peg$currPos) === 93) {
                  s9 = peg$c86;
                  peg$currPos++;
                } else {
                  s9 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c87); }
                }
                if (s9 !== peg$FAILED) {
                  peg$savedPos = s3;
                  s4 = peg$c263(s1, s7);
                  s3 = s4;
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      } else {
        peg$currPos = s3;
        s3 = peg$FAILED;
      }
      if (s3 === peg$FAILED) {
        s3 = peg$currPos;
        s4 = peg$parse__();
        if (s4 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 46) {
            s5 = peg$c81;
            peg$currPos++;
          } else {
            s5 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c82); }
          }
          if (s5 !== peg$FAILED) {
            s6 = peg$parse__();
            if (s6 !== peg$FAILED) {
              s7 = peg$parsefieldName();
              if (s7 !== peg$FAILED) {
                peg$savedPos = s3;
                s4 = peg$c264(s1, s7);
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
      }
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$currPos;
        s4 = peg$parse__();
        if (s4 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 91) {
            s5 = peg$c84;
            peg$currPos++;
          } else {
            s5 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c85); }
          }
          if (s5 !== peg$FAILED) {
            s6 = peg$parse__();
            if (s6 !== peg$FAILED) {
              s7 = peg$parseConditionalExpression();
              if (s7 !== peg$FAILED) {
                s8 = peg$parse__();
                if (s8 !== peg$FAILED) {
                  if (input.charCodeAt(peg$currPos) === 93) {
                    s9 = peg$c86;
                    peg$currPos++;
                  } else {
                    s9 = peg$FAILED;
                    if (peg$silentFails === 0) { peg$fail(peg$c87); }
                  }
                  if (s9 !== peg$FAILED) {
                    peg$savedPos = s3;
                    s4 = peg$c263(s1, s7);
                    s3 = s4;
                  } else {
                    peg$currPos = s3;
                    s3 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        if (s3 === peg$FAILED) {
          s3 = peg$currPos;
          s4 = peg$parse__();
          if (s4 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 46) {
              s5 = peg$c81;
              peg$currPos++;
            } else {
              s5 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c82); }
            }
            if (s5 !== peg$FAILED) {
              s6 = peg$parse__();
              if (s6 !== peg$FAILED) {
                s7 = peg$parsefieldName();
                if (s7 !== peg$FAILED) {
                  peg$savedPos = s3;
                  s4 = peg$c264(s1, s7);
                  s3 = s4;
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        }
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c265(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseduration() {
    var s0, s1, s2, s3, s4, s5;

    s0 = peg$parseseconds();
    if (s0 === peg$FAILED) {
      s0 = peg$parseminutes();
      if (s0 === peg$FAILED) {
        s0 = peg$parsehours();
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parsehours();
          if (s1 !== peg$FAILED) {
            s2 = peg$parse_();
            if (s2 !== peg$FAILED) {
              if (input.substr(peg$currPos, 3) === peg$c68) {
                s3 = peg$c68;
                peg$currPos += 3;
              } else {
                s3 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c266); }
              }
              if (s3 !== peg$FAILED) {
                s4 = peg$parse_();
                if (s4 !== peg$FAILED) {
                  s5 = peg$parseminutes();
                  if (s5 !== peg$FAILED) {
                    s1 = [s1, s2, s3, s4, s5];
                    s0 = s1;
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
          if (s0 === peg$FAILED) {
            s0 = peg$parsedays();
            if (s0 === peg$FAILED) {
              s0 = peg$parseweeks();
            }
          }
        }
      }
    }

    return s0;
  }

  function peg$parsesec_abbrev() {
    var s0;

    if (input.substr(peg$currPos, 7) === peg$c267) {
      s0 = peg$c267;
      peg$currPos += 7;
    } else {
      s0 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c268); }
    }
    if (s0 === peg$FAILED) {
      if (input.substr(peg$currPos, 6) === peg$c269) {
        s0 = peg$c269;
        peg$currPos += 6;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c270); }
      }
      if (s0 === peg$FAILED) {
        if (input.substr(peg$currPos, 4) === peg$c271) {
          s0 = peg$c271;
          peg$currPos += 4;
        } else {
          s0 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c272); }
        }
        if (s0 === peg$FAILED) {
          if (input.substr(peg$currPos, 3) === peg$c273) {
            s0 = peg$c273;
            peg$currPos += 3;
          } else {
            s0 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c274); }
          }
          if (s0 === peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 115) {
              s0 = peg$c275;
              peg$currPos++;
            } else {
              s0 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c276); }
            }
          }
        }
      }
    }

    return s0;
  }

  function peg$parsemin_abbrev() {
    var s0;

    if (input.substr(peg$currPos, 7) === peg$c277) {
      s0 = peg$c277;
      peg$currPos += 7;
    } else {
      s0 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c278); }
    }
    if (s0 === peg$FAILED) {
      if (input.substr(peg$currPos, 6) === peg$c279) {
        s0 = peg$c279;
        peg$currPos += 6;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c280); }
      }
      if (s0 === peg$FAILED) {
        if (input.substr(peg$currPos, 4) === peg$c281) {
          s0 = peg$c281;
          peg$currPos += 4;
        } else {
          s0 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c282); }
        }
        if (s0 === peg$FAILED) {
          if (input.substr(peg$currPos, 3) === peg$c116) {
            s0 = peg$c116;
            peg$currPos += 3;
          } else {
            s0 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c283); }
          }
          if (s0 === peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 109) {
              s0 = peg$c284;
              peg$currPos++;
            } else {
              s0 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c285); }
            }
          }
        }
      }
    }

    return s0;
  }

  function peg$parsehour_abbrev() {
    var s0;

    if (input.substr(peg$currPos, 5) === peg$c286) {
      s0 = peg$c286;
      peg$currPos += 5;
    } else {
      s0 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c287); }
    }
    if (s0 === peg$FAILED) {
      if (input.substr(peg$currPos, 3) === peg$c288) {
        s0 = peg$c288;
        peg$currPos += 3;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c289); }
      }
      if (s0 === peg$FAILED) {
        if (input.substr(peg$currPos, 2) === peg$c290) {
          s0 = peg$c290;
          peg$currPos += 2;
        } else {
          s0 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c291); }
        }
        if (s0 === peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 104) {
            s0 = peg$c292;
            peg$currPos++;
          } else {
            s0 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c293); }
          }
          if (s0 === peg$FAILED) {
            if (input.substr(peg$currPos, 4) === peg$c294) {
              s0 = peg$c294;
              peg$currPos += 4;
            } else {
              s0 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c295); }
            }
          }
        }
      }
    }

    return s0;
  }

  function peg$parseday_abbrev() {
    var s0;

    if (input.substr(peg$currPos, 4) === peg$c296) {
      s0 = peg$c296;
      peg$currPos += 4;
    } else {
      s0 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c297); }
    }
    if (s0 === peg$FAILED) {
      if (input.substr(peg$currPos, 3) === peg$c298) {
        s0 = peg$c298;
        peg$currPos += 3;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c299); }
      }
      if (s0 === peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 100) {
          s0 = peg$c300;
          peg$currPos++;
        } else {
          s0 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c301); }
        }
      }
    }

    return s0;
  }

  function peg$parseweek_abbrev() {
    var s0;

    if (input.substr(peg$currPos, 5) === peg$c302) {
      s0 = peg$c302;
      peg$currPos += 5;
    } else {
      s0 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c303); }
    }
    if (s0 === peg$FAILED) {
      if (input.substr(peg$currPos, 4) === peg$c304) {
        s0 = peg$c304;
        peg$currPos += 4;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c305); }
      }
      if (s0 === peg$FAILED) {
        if (input.substr(peg$currPos, 3) === peg$c306) {
          s0 = peg$c306;
          peg$currPos += 3;
        } else {
          s0 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c307); }
        }
        if (s0 === peg$FAILED) {
          if (input.substr(peg$currPos, 2) === peg$c308) {
            s0 = peg$c308;
            peg$currPos += 2;
          } else {
            s0 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c309); }
          }
          if (s0 === peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 119) {
              s0 = peg$c310;
              peg$currPos++;
            } else {
              s0 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c311); }
            }
          }
        }
      }
    }

    return s0;
  }

  function peg$parseseconds() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 6) === peg$c269) {
      s1 = peg$c269;
      peg$currPos += 6;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c270); }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c312();
    }
    s0 = s1;
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      s1 = peg$parseunsignedInteger();
      if (s1 !== peg$FAILED) {
        s2 = peg$parse_();
        if (s2 === peg$FAILED) {
          s2 = null;
        }
        if (s2 !== peg$FAILED) {
          s3 = peg$parsesec_abbrev();
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c313(s1);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    }

    return s0;
  }

  function peg$parseminutes() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 6) === peg$c279) {
      s1 = peg$c279;
      peg$currPos += 6;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c280); }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c314();
    }
    s0 = s1;
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      s1 = peg$parseunsignedInteger();
      if (s1 !== peg$FAILED) {
        s2 = peg$parse_();
        if (s2 === peg$FAILED) {
          s2 = null;
        }
        if (s2 !== peg$FAILED) {
          s3 = peg$parsemin_abbrev();
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c315(s1);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    }

    return s0;
  }

  function peg$parsehours() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 4) === peg$c294) {
      s1 = peg$c294;
      peg$currPos += 4;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c295); }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c316();
    }
    s0 = s1;
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      s1 = peg$parseunsignedInteger();
      if (s1 !== peg$FAILED) {
        s2 = peg$parse_();
        if (s2 === peg$FAILED) {
          s2 = null;
        }
        if (s2 !== peg$FAILED) {
          s3 = peg$parsehour_abbrev();
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c317(s1);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    }

    return s0;
  }

  function peg$parsedays() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 3) === peg$c298) {
      s1 = peg$c298;
      peg$currPos += 3;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c299); }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c318();
    }
    s0 = s1;
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      s1 = peg$parseunsignedInteger();
      if (s1 !== peg$FAILED) {
        s2 = peg$parse_();
        if (s2 === peg$FAILED) {
          s2 = null;
        }
        if (s2 !== peg$FAILED) {
          s3 = peg$parseday_abbrev();
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c319(s1);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    }

    return s0;
  }

  function peg$parseweeks() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    s1 = peg$parseunsignedInteger();
    if (s1 !== peg$FAILED) {
      s2 = peg$parse_();
      if (s2 === peg$FAILED) {
        s2 = null;
      }
      if (s2 !== peg$FAILED) {
        s3 = peg$parseweek_abbrev();
        if (s3 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c320(s1);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseaddr() {
    var s0, s1, s2, s3, s4, s5, s6, s7, s8;

    s0 = peg$currPos;
    s1 = peg$currPos;
    s2 = peg$parseunsignedInteger();
    if (s2 !== peg$FAILED) {
      if (input.charCodeAt(peg$currPos) === 46) {
        s3 = peg$c81;
        peg$currPos++;
      } else {
        s3 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c82); }
      }
      if (s3 !== peg$FAILED) {
        s4 = peg$parseunsignedInteger();
        if (s4 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 46) {
            s5 = peg$c81;
            peg$currPos++;
          } else {
            s5 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c82); }
          }
          if (s5 !== peg$FAILED) {
            s6 = peg$parseunsignedInteger();
            if (s6 !== peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 46) {
                s7 = peg$c81;
                peg$currPos++;
              } else {
                s7 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c82); }
              }
              if (s7 !== peg$FAILED) {
                s8 = peg$parseunsignedInteger();
                if (s8 !== peg$FAILED) {
                  s2 = [s2, s3, s4, s5, s6, s7, s8];
                  s1 = s2;
                } else {
                  peg$currPos = s1;
                  s1 = peg$FAILED;
                }
              } else {
                peg$currPos = s1;
                s1 = peg$FAILED;
              }
            } else {
              peg$currPos = s1;
              s1 = peg$FAILED;
            }
          } else {
            peg$currPos = s1;
            s1 = peg$FAILED;
          }
        } else {
          peg$currPos = s1;
          s1 = peg$FAILED;
        }
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
    } else {
      peg$currPos = s1;
      s1 = peg$FAILED;
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c321();
    }
    s0 = s1;

    return s0;
  }

  function peg$parseport() {
    var s0, s1, s2;

    s0 = peg$currPos;
    if (input.charCodeAt(peg$currPos) === 58) {
      s1 = peg$c197;
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c198); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parsesuint();
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c36(s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseip6addr() {
    var s0, s1, s2, s3, s4, s5;

    s0 = peg$currPos;
    s1 = [];
    s2 = peg$parseh_prepend();
    if (s2 !== peg$FAILED) {
      while (s2 !== peg$FAILED) {
        s1.push(s2);
        s2 = peg$parseh_prepend();
      }
    } else {
      s1 = peg$FAILED;
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parseip6tail();
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c322(s1, s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      s1 = peg$parseh16();
      if (s1 !== peg$FAILED) {
        s2 = [];
        s3 = peg$parseh_append();
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          s3 = peg$parseh_append();
        }
        if (s2 !== peg$FAILED) {
          if (input.substr(peg$currPos, 2) === peg$c323) {
            s3 = peg$c323;
            peg$currPos += 2;
          } else {
            s3 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c324); }
          }
          if (s3 !== peg$FAILED) {
            s4 = [];
            s5 = peg$parseh_prepend();
            while (s5 !== peg$FAILED) {
              s4.push(s5);
              s5 = peg$parseh_prepend();
            }
            if (s4 !== peg$FAILED) {
              s5 = peg$parseip6tail();
              if (s5 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c325(s1, s2, s4, s5);
                s0 = s1;
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        if (input.substr(peg$currPos, 2) === peg$c323) {
          s1 = peg$c323;
          peg$currPos += 2;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c324); }
        }
        if (s1 !== peg$FAILED) {
          s2 = [];
          s3 = peg$parseh_prepend();
          while (s3 !== peg$FAILED) {
            s2.push(s3);
            s3 = peg$parseh_prepend();
          }
          if (s2 !== peg$FAILED) {
            s3 = peg$parseip6tail();
            if (s3 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c326(s2, s3);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseh16();
          if (s1 !== peg$FAILED) {
            s2 = [];
            s3 = peg$parseh_append();
            while (s3 !== peg$FAILED) {
              s2.push(s3);
              s3 = peg$parseh_append();
            }
            if (s2 !== peg$FAILED) {
              if (input.substr(peg$currPos, 2) === peg$c323) {
                s3 = peg$c323;
                peg$currPos += 2;
              } else {
                s3 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c324); }
              }
              if (s3 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c327(s1, s2);
                s0 = s1;
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            if (input.substr(peg$currPos, 2) === peg$c323) {
              s1 = peg$c323;
              peg$currPos += 2;
            } else {
              s1 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c324); }
            }
            if (s1 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c328();
            }
            s0 = s1;
          }
        }
      }
    }

    return s0;
  }

  function peg$parseip6tail() {
    var s0;

    s0 = peg$parseaddr();
    if (s0 === peg$FAILED) {
      s0 = peg$parseh16();
    }

    return s0;
  }

  function peg$parseh_append() {
    var s0, s1, s2;

    s0 = peg$currPos;
    if (input.charCodeAt(peg$currPos) === 58) {
      s1 = peg$c197;
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c198); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parseh16();
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c329(s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseh_prepend() {
    var s0, s1, s2;

    s0 = peg$currPos;
    s1 = peg$parseh16();
    if (s1 !== peg$FAILED) {
      if (input.charCodeAt(peg$currPos) === 58) {
        s2 = peg$c197;
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c198); }
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c330(s1);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsesubnet() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    s1 = peg$parseaddr();
    if (s1 !== peg$FAILED) {
      if (input.charCodeAt(peg$currPos) === 47) {
        s2 = peg$c220;
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c221); }
      }
      if (s2 !== peg$FAILED) {
        s3 = peg$parseunsignedInteger();
        if (s3 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c331(s1, s3);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseip6subnet() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    s1 = peg$parseip6addr();
    if (s1 !== peg$FAILED) {
      if (input.charCodeAt(peg$currPos) === 47) {
        s2 = peg$c220;
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c221); }
      }
      if (s2 !== peg$FAILED) {
        s3 = peg$parseunsignedInteger();
        if (s3 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c332(s1, s3);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseunsignedInteger() {
    var s0, s1;

    s0 = peg$currPos;
    s1 = peg$parsesuint();
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c333(s1);
    }
    s0 = s1;

    return s0;
  }

  function peg$parsesuint() {
    var s0, s1, s2;

    s0 = peg$currPos;
    s1 = [];
    if (peg$c79.test(input.charAt(peg$currPos))) {
      s2 = input.charAt(peg$currPos);
      peg$currPos++;
    } else {
      s2 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c80); }
    }
    if (s2 !== peg$FAILED) {
      while (s2 !== peg$FAILED) {
        s1.push(s2);
        if (peg$c79.test(input.charAt(peg$currPos))) {
          s2 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c80); }
        }
      }
    } else {
      s1 = peg$FAILED;
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c70();
    }
    s0 = s1;

    return s0;
  }

  function peg$parsesinteger() {
    var s0, s1, s2;

    s0 = peg$currPos;
    if (peg$c334.test(input.charAt(peg$currPos))) {
      s1 = input.charAt(peg$currPos);
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c335); }
    }
    if (s1 === peg$FAILED) {
      s1 = null;
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parsesuint();
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c70();
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsesdouble() {
    var s0, s1, s2, s3, s4, s5;

    s0 = peg$currPos;
    if (input.charCodeAt(peg$currPos) === 45) {
      s1 = peg$c16;
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c17); }
    }
    if (s1 === peg$FAILED) {
      s1 = null;
    }
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$parsedoubleInteger();
      if (s3 !== peg$FAILED) {
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          s3 = peg$parsedoubleInteger();
        }
      } else {
        s2 = peg$FAILED;
      }
      if (s2 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 46) {
          s3 = peg$c81;
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c82); }
        }
        if (s3 !== peg$FAILED) {
          s4 = [];
          s5 = peg$parsedoubleDigit();
          if (s5 !== peg$FAILED) {
            while (s5 !== peg$FAILED) {
              s4.push(s5);
              s5 = peg$parsedoubleDigit();
            }
          } else {
            s4 = peg$FAILED;
          }
          if (s4 !== peg$FAILED) {
            s5 = peg$parseexponentPart();
            if (s5 === peg$FAILED) {
              s5 = null;
            }
            if (s5 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c337();
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 45) {
        s1 = peg$c16;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c17); }
      }
      if (s1 === peg$FAILED) {
        s1 = null;
      }
      if (s1 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 46) {
          s2 = peg$c81;
          peg$currPos++;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c82); }
        }
        if (s2 !== peg$FAILED) {
          s3 = [];
          s4 = peg$parsedoubleDigit();
          if (s4 !== peg$FAILED) {
            while (s4 !== peg$FAILED) {
              s3.push(s4);
              s4 = peg$parsedoubleDigit();
            }
          } else {
            s3 = peg$FAILED;
          }
          if (s3 !== peg$FAILED) {
            s4 = peg$parseexponentPart();
            if (s4 === peg$FAILED) {
              s4 = null;
            }
            if (s4 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c337();
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    }

    return s0;
  }

  function peg$parsedoubleInteger() {
    var s0, s1, s2, s3;

    if (input.charCodeAt(peg$currPos) === 48) {
      s0 = peg$c338;
      peg$currPos++;
    } else {
      s0 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c339); }
    }
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      if (peg$c340.test(input.charAt(peg$currPos))) {
        s1 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c341); }
      }
      if (s1 !== peg$FAILED) {
        s2 = [];
        if (peg$c79.test(input.charAt(peg$currPos))) {
          s3 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c80); }
        }
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          if (peg$c79.test(input.charAt(peg$currPos))) {
            s3 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s3 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c80); }
          }
        }
        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    }

    return s0;
  }

  function peg$parsedoubleDigit() {
    var s0;

    if (peg$c79.test(input.charAt(peg$currPos))) {
      s0 = input.charAt(peg$currPos);
      peg$currPos++;
    } else {
      s0 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c80); }
    }

    return s0;
  }

  function peg$parseexponentPart() {
    var s0, s1, s2;

    s0 = peg$currPos;
    if (input.substr(peg$currPos, 1).toLowerCase() === peg$c342) {
      s1 = input.charAt(peg$currPos);
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c343); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parsesinteger();
      if (s2 !== peg$FAILED) {
        s1 = [s1, s2];
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parseh16() {
    var s0, s1, s2;

    s0 = peg$currPos;
    s1 = [];
    s2 = peg$parsehexdigit();
    if (s2 !== peg$FAILED) {
      while (s2 !== peg$FAILED) {
        s1.push(s2);
        s2 = peg$parsehexdigit();
      }
    } else {
      s1 = peg$FAILED;
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c344();
    }
    s0 = s1;

    return s0;
  }

  function peg$parsehexdigit() {
    var s0;

    if (peg$c345.test(input.charAt(peg$currPos))) {
      s0 = input.charAt(peg$currPos);
      peg$currPos++;
    } else {
      s0 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c346); }
    }

    return s0;
  }

  function peg$parsesearchWord() {
    var s0, s1, s2;

    s0 = peg$currPos;
    s1 = [];
    s2 = peg$parsesearchWordPart();
    if (s2 !== peg$FAILED) {
      while (s2 !== peg$FAILED) {
        s1.push(s2);
        s2 = peg$parsesearchWordPart();
      }
    } else {
      s1 = peg$FAILED;
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c347(s1);
    }
    s0 = s1;

    return s0;
  }

  function peg$parsesearchWordPart() {
    var s0, s1, s2;

    s0 = peg$currPos;
    if (input.charCodeAt(peg$currPos) === 92) {
      s1 = peg$c348;
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c349); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parseescapeSequence();
      if (s2 === peg$FAILED) {
        s2 = peg$parsesearchEscape();
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c18(s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      s1 = peg$currPos;
      peg$silentFails++;
      if (peg$c350.test(input.charAt(peg$currPos))) {
        s2 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c351); }
      }
      if (s2 === peg$FAILED) {
        s2 = peg$parsews();
      }
      peg$silentFails--;
      if (s2 === peg$FAILED) {
        s1 = void 0;
      } else {
        peg$currPos = s1;
        s1 = peg$FAILED;
      }
      if (s1 !== peg$FAILED) {
        if (input.length > peg$currPos) {
          s2 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c352); }
        }
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c70();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    }

    return s0;
  }

  function peg$parsequotedString() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    if (input.charCodeAt(peg$currPos) === 34) {
      s1 = peg$c353;
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c354); }
    }
    if (s1 !== peg$FAILED) {
      s2 = [];
      s3 = peg$parsedoubleQuotedChar();
      while (s3 !== peg$FAILED) {
        s2.push(s3);
        s3 = peg$parsedoubleQuotedChar();
      }
      if (s2 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 34) {
          s3 = peg$c353;
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c354); }
        }
        if (s3 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c355(s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 39) {
        s1 = peg$c356;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c357); }
      }
      if (s1 !== peg$FAILED) {
        s2 = [];
        s3 = peg$parsesingleQuotedChar();
        while (s3 !== peg$FAILED) {
          s2.push(s3);
          s3 = peg$parsesingleQuotedChar();
        }
        if (s2 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 39) {
            s3 = peg$c356;
            peg$currPos++;
          } else {
            s3 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c357); }
          }
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c355(s2);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    }

    return s0;
  }

  function peg$parsedoubleQuotedChar() {
    var s0, s1, s2;

    s0 = peg$currPos;
    s1 = peg$currPos;
    peg$silentFails++;
    if (input.charCodeAt(peg$currPos) === 34) {
      s2 = peg$c353;
      peg$currPos++;
    } else {
      s2 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c354); }
    }
    if (s2 === peg$FAILED) {
      s2 = peg$parseescapedChar();
    }
    peg$silentFails--;
    if (s2 === peg$FAILED) {
      s1 = void 0;
    } else {
      peg$currPos = s1;
      s1 = peg$FAILED;
    }
    if (s1 !== peg$FAILED) {
      if (input.length > peg$currPos) {
        s2 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c352); }
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c70();
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 92) {
        s1 = peg$c348;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c349); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$parseescapeSequence();
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c18(s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    }

    return s0;
  }

  function peg$parsesingleQuotedChar() {
    var s0, s1, s2;

    s0 = peg$currPos;
    s1 = peg$currPos;
    peg$silentFails++;
    if (input.charCodeAt(peg$currPos) === 39) {
      s2 = peg$c356;
      peg$currPos++;
    } else {
      s2 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c357); }
    }
    if (s2 === peg$FAILED) {
      s2 = peg$parseescapedChar();
    }
    peg$silentFails--;
    if (s2 === peg$FAILED) {
      s1 = void 0;
    } else {
      peg$currPos = s1;
      s1 = peg$FAILED;
    }
    if (s1 !== peg$FAILED) {
      if (input.length > peg$currPos) {
        s2 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c352); }
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c70();
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 92) {
        s1 = peg$c348;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c349); }
      }
      if (s1 !== peg$FAILED) {
        s2 = peg$parseescapeSequence();
        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c18(s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    }

    return s0;
  }

  function peg$parseescapeSequence() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    if (input.charCodeAt(peg$currPos) === 120) {
      s1 = peg$c358;
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c359); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parsehexdigit();
      if (s2 !== peg$FAILED) {
        s3 = peg$parsehexdigit();
        if (s3 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c360();
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$parsesingleCharEscape();
      if (s0 === peg$FAILED) {
        s0 = peg$parseunicodeEscape();
      }
    }

    return s0;
  }

  function peg$parsesingleCharEscape() {
    var s0, s1;

    if (input.charCodeAt(peg$currPos) === 39) {
      s0 = peg$c356;
      peg$currPos++;
    } else {
      s0 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c357); }
    }
    if (s0 === peg$FAILED) {
      if (input.charCodeAt(peg$currPos) === 34) {
        s0 = peg$c353;
        peg$currPos++;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c354); }
      }
      if (s0 === peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 92) {
          s0 = peg$c348;
          peg$currPos++;
        } else {
          s0 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c349); }
        }
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          if (input.charCodeAt(peg$currPos) === 98) {
            s1 = peg$c361;
            peg$currPos++;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c362); }
          }
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c363();
          }
          s0 = s1;
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            if (input.charCodeAt(peg$currPos) === 102) {
              s1 = peg$c364;
              peg$currPos++;
            } else {
              s1 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c365); }
            }
            if (s1 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c366();
            }
            s0 = s1;
            if (s0 === peg$FAILED) {
              s0 = peg$currPos;
              if (input.charCodeAt(peg$currPos) === 110) {
                s1 = peg$c367;
                peg$currPos++;
              } else {
                s1 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c368); }
              }
              if (s1 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c369();
              }
              s0 = s1;
              if (s0 === peg$FAILED) {
                s0 = peg$currPos;
                if (input.charCodeAt(peg$currPos) === 114) {
                  s1 = peg$c370;
                  peg$currPos++;
                } else {
                  s1 = peg$FAILED;
                  if (peg$silentFails === 0) { peg$fail(peg$c371); }
                }
                if (s1 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$c372();
                }
                s0 = s1;
                if (s0 === peg$FAILED) {
                  s0 = peg$currPos;
                  if (input.charCodeAt(peg$currPos) === 116) {
                    s1 = peg$c373;
                    peg$currPos++;
                  } else {
                    s1 = peg$FAILED;
                    if (peg$silentFails === 0) { peg$fail(peg$c374); }
                  }
                  if (s1 !== peg$FAILED) {
                    peg$savedPos = s0;
                    s1 = peg$c375();
                  }
                  s0 = s1;
                  if (s0 === peg$FAILED) {
                    s0 = peg$currPos;
                    if (input.charCodeAt(peg$currPos) === 118) {
                      s1 = peg$c376;
                      peg$currPos++;
                    } else {
                      s1 = peg$FAILED;
                      if (peg$silentFails === 0) { peg$fail(peg$c377); }
                    }
                    if (s1 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s1 = peg$c378();
                    }
                    s0 = s1;
                  }
                }
              }
            }
          }
        }
      }
    }

    return s0;
  }

  function peg$parsesearchEscape() {
    var s0, s1;

    s0 = peg$currPos;
    if (input.charCodeAt(peg$currPos) === 61) {
      s1 = peg$c135;
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c136); }
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c379();
    }
    s0 = s1;
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 42) {
        s1 = peg$c24;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c25); }
      }
      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c380();
      }
      s0 = s1;
    }

    return s0;
  }

  function peg$parseunicodeEscape() {
    var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9;

    s0 = peg$currPos;
    if (input.charCodeAt(peg$currPos) === 117) {
      s1 = peg$c381;
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c382); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$currPos;
      s3 = peg$parsehexdigit();
      if (s3 !== peg$FAILED) {
        s4 = peg$parsehexdigit();
        if (s4 !== peg$FAILED) {
          s5 = peg$parsehexdigit();
          if (s5 !== peg$FAILED) {
            s6 = peg$parsehexdigit();
            if (s6 !== peg$FAILED) {
              s3 = [s3, s4, s5, s6];
              s2 = s3;
            } else {
              peg$currPos = s2;
              s2 = peg$FAILED;
            }
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
      } else {
        peg$currPos = s2;
        s2 = peg$FAILED;
      }
      if (s2 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c383(s2);
        s0 = s1;
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }
    if (s0 === peg$FAILED) {
      s0 = peg$currPos;
      if (input.charCodeAt(peg$currPos) === 117) {
        s1 = peg$c381;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c382); }
      }
      if (s1 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 123) {
          s2 = peg$c384;
          peg$currPos++;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c385); }
        }
        if (s2 !== peg$FAILED) {
          s3 = peg$currPos;
          s4 = peg$parsehexdigit();
          if (s4 !== peg$FAILED) {
            s5 = peg$parsehexdigit();
            if (s5 === peg$FAILED) {
              s5 = null;
            }
            if (s5 !== peg$FAILED) {
              s6 = peg$parsehexdigit();
              if (s6 === peg$FAILED) {
                s6 = null;
              }
              if (s6 !== peg$FAILED) {
                s7 = peg$parsehexdigit();
                if (s7 === peg$FAILED) {
                  s7 = null;
                }
                if (s7 !== peg$FAILED) {
                  s8 = peg$parsehexdigit();
                  if (s8 === peg$FAILED) {
                    s8 = null;
                  }
                  if (s8 !== peg$FAILED) {
                    s9 = peg$parsehexdigit();
                    if (s9 === peg$FAILED) {
                      s9 = null;
                    }
                    if (s9 !== peg$FAILED) {
                      s4 = [s4, s5, s6, s7, s8, s9];
                      s3 = s4;
                    } else {
                      peg$currPos = s3;
                      s3 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s3;
                    s3 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s3;
                  s3 = peg$FAILED;
                }
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
          if (s3 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 125) {
              s4 = peg$c386;
              peg$currPos++;
            } else {
              s4 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c387); }
            }
            if (s4 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c383(s3);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    }

    return s0;
  }

  function peg$parsereString() {
    var s0, s1, s2, s3;

    s0 = peg$currPos;
    if (input.charCodeAt(peg$currPos) === 47) {
      s1 = peg$c220;
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c221); }
    }
    if (s1 !== peg$FAILED) {
      s2 = peg$parsereBody();
      if (s2 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 47) {
          s3 = peg$c220;
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c221); }
        }
        if (s3 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c36(s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }

  function peg$parsereBody() {
    var s0, s1, s2;

    s0 = peg$currPos;
    s1 = [];
    if (peg$c388.test(input.charAt(peg$currPos))) {
      s2 = input.charAt(peg$currPos);
      peg$currPos++;
    } else {
      s2 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c389); }
    }
    if (s2 === peg$FAILED) {
      if (input.substr(peg$currPos, 2) === peg$c390) {
        s2 = peg$c390;
        peg$currPos += 2;
      } else {
        s2 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c391); }
      }
    }
    if (s2 !== peg$FAILED) {
      while (s2 !== peg$FAILED) {
        s1.push(s2);
        if (peg$c388.test(input.charAt(peg$currPos))) {
          s2 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c389); }
        }
        if (s2 === peg$FAILED) {
          if (input.substr(peg$currPos, 2) === peg$c390) {
            s2 = peg$c390;
            peg$currPos += 2;
          } else {
            s2 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c391); }
          }
        }
      }
    } else {
      s1 = peg$FAILED;
    }
    if (s1 !== peg$FAILED) {
      peg$savedPos = s0;
      s1 = peg$c70();
    }
    s0 = s1;

    return s0;
  }

  function peg$parseescapedChar() {
    var s0;

    if (peg$c392.test(input.charAt(peg$currPos))) {
      s0 = input.charAt(peg$currPos);
      peg$currPos++;
    } else {
      s0 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c393); }
    }

    return s0;
  }

  function peg$parsews() {
    var s0;

    if (input.charCodeAt(peg$currPos) === 9) {
      s0 = peg$c394;
      peg$currPos++;
    } else {
      s0 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c395); }
    }
    if (s0 === peg$FAILED) {
      if (input.charCodeAt(peg$currPos) === 11) {
        s0 = peg$c396;
        peg$currPos++;
      } else {
        s0 = peg$FAILED;
        if (peg$silentFails === 0) { peg$fail(peg$c397); }
      }
      if (s0 === peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 12) {
          s0 = peg$c398;
          peg$currPos++;
        } else {
          s0 = peg$FAILED;
          if (peg$silentFails === 0) { peg$fail(peg$c399); }
        }
        if (s0 === peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 32) {
            s0 = peg$c400;
            peg$currPos++;
          } else {
            s0 = peg$FAILED;
            if (peg$silentFails === 0) { peg$fail(peg$c401); }
          }
          if (s0 === peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 160) {
              s0 = peg$c402;
              peg$currPos++;
            } else {
              s0 = peg$FAILED;
              if (peg$silentFails === 0) { peg$fail(peg$c403); }
            }
            if (s0 === peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 65279) {
                s0 = peg$c404;
                peg$currPos++;
              } else {
                s0 = peg$FAILED;
                if (peg$silentFails === 0) { peg$fail(peg$c405); }
              }
            }
          }
        }
      }
    }

    return s0;
  }

  function peg$parse_() {
    var s0, s1;

    peg$silentFails++;
    s0 = [];
    s1 = peg$parsews();
    if (s1 !== peg$FAILED) {
      while (s1 !== peg$FAILED) {
        s0.push(s1);
        s1 = peg$parsews();
      }
    } else {
      s0 = peg$FAILED;
    }
    peg$silentFails--;
    if (s0 === peg$FAILED) {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c406); }
    }

    return s0;
  }

  function peg$parse__() {
    var s0, s1;

    s0 = [];
    s1 = peg$parsews();
    while (s1 !== peg$FAILED) {
      s0.push(s1);
      s1 = peg$parsews();
    }

    return s0;
  }

  function peg$parseEOF() {
    var s0, s1;

    s0 = peg$currPos;
    peg$silentFails++;
    if (input.length > peg$currPos) {
      s1 = input.charAt(peg$currPos);
      peg$currPos++;
    } else {
      s1 = peg$FAILED;
      if (peg$silentFails === 0) { peg$fail(peg$c352); }
    }
    peg$silentFails--;
    if (s1 === peg$FAILED) {
      s0 = void 0;
    } else {
      peg$currPos = s0;
      s0 = peg$FAILED;
    }

    return s0;
  }



  let reglob$1 = reglob;

  function makeChain(first, rest, op) {
    if (!rest || rest.length == 0) {
      return first;
    }
    let result = first;
    for (let term of rest) {
      result = { op, left: result, right: term };
    }
    return result;
  }

  function makeArgMap(args) {
    let m = {};
    for (let arg of args) {
      if (arg.name in m) {
        throw new Error(`Duplicate argument -${arg.name}`);
      }
      m[arg.name] = arg.value;
    }
    return m
  }

  function makeBinaryExprChain(first, rest) {
    let ret = first;
    for (let part of rest) {
      ret = { op: "BinaryExpr", operator: part[0], lhs: ret, rhs: part[1] };
    }
    return ret
  }

  function joinChars(chars) {
    return chars.join("");
  }

  function toLowerCase(str) {
    return str.toLowerCase();
  }

  function OR(a, b) {
    return a || b
  }

  function makeUnicodeChar(chars) {
    let n = parseInt(chars.join(""), 16);
    if (n < 0x10000) {
      return String.fromCharCode(n);
    }

    // stupid javascript 16 bit code points...
    n -= 0x10000;
    let surrogate1 = 0xD800 + ((n >> 10) & 0x7ff);
    let surrogate2 = 0xDC00 + (n & 0x3ff);
    return String.fromCharCode(surrogate1) + String.fromCharCode(surrogate2);
  }



  peg$result = peg$startRuleFunction();

  if (peg$result !== peg$FAILED && peg$currPos === input.length) {
    return peg$result;
  } else {
    if (peg$result !== peg$FAILED && peg$currPos < input.length) {
      peg$fail(peg$endExpectation());
    }

    throw peg$buildStructuredError(
      peg$maxFailExpected,
      peg$maxFailPos < input.length ? input.charAt(peg$maxFailPos) : null,
      peg$maxFailPos < input.length
        ? peg$computeLocation(peg$maxFailPos, peg$maxFailPos + 1)
        : peg$computeLocation(peg$maxFailPos, peg$maxFailPos)
    );
  }
}

var zql = {
  SyntaxError: peg$SyntaxError,
  parse:       peg$parse
};

export default zql;
