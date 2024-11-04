package radix

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"github.com/mozillazg/go-pinyin"
)

type index_char struct {
	CharStr  string
	CharType int
}

func (ic *index_char) is_han_char() bool {
	return ic.CharType == 0
}

type index_phrase struct {
	index_chars []*index_char
}

func _index_chars_to_str(chars []*index_char) string {
	result := ""
	for _, c := range chars {
		result += c.CharStr
		if !c.is_han_char() {
			result += " "
		}
	}
	return strings.TrimSpace(result)
}

func (is *index_phrase) ToString() string {
	return _index_chars_to_str(is.index_chars)
}

func (is *index_phrase) ToPinyin() string {
	return word_to_pinyin(_index_chars_to_str(is.index_chars))
}

func _index_chars_length(chars []*index_char) int {
	count := 0
	for _, c := range chars {
		if c.is_han_char() {
			for _, r := range c.CharStr {
				if unicode.Is(unicode.Han, r) {
					count += 2
				} else {
					count += 1
				}
			}
		} else {
			count += len(c.CharStr)
		}
	}
	return count
}

func (is *index_phrase) Length() int {
	return _index_chars_length(is.index_chars)
}

func extractEndingDigits(s string, endingDigits int) (string, bool) {
	// 定义正则表达式，匹配以6位或更多数字结尾的字符串
	pattern := fmt.Sprintf(`(\d{%d,})$`, endingDigits)
	re := regexp.MustCompile(pattern)
	match := re.FindStringSubmatch(s)
	if len(match) > 1 {
		return match[1], true
	}
	return "", false
}

func (is *index_phrase) IndexPhraseTrim(prefixes []string, suffixes []string, endingDigits int) {
	phrase_str := is.ToString()
	if endingDigits > 0 {
		ending, ok := extractEndingDigits(phrase_str, endingDigits)
		if ok {
			is.index_chars = to_index_chars(ending)
			return
		}
	}

	for _, prefix := range prefixes {
		if strings.HasPrefix(phrase_str, prefix) {
			phrase_str = strings.TrimPrefix(phrase_str, prefix)
			break
		}
	}
	for _, suffix := range suffixes {
		if strings.HasSuffix(phrase_str, suffix) {
			phrase_str = strings.TrimSuffix(phrase_str, suffix)
			break
		}
	}
	is.index_chars = to_index_chars(phrase_str)
}

func (is *index_phrase) SplitToIndexWords(maskCount int, outOfOrder bool) []string {

	if is.Length() < 4 {
		return []string{is.ToString()}
	}

	phrase_str := is.ToString()
	split_words := make(map[string]bool)

	// 1. 自身作为一个索引词
	split_words[phrase_str] = true

	// 2. 前缀切词，每少一个char，作为一个索引词，直至切出的索引词长度小于4
	for i := 1; i < len(is.index_chars)-1; i++ {
		sub_chars := is.index_chars[i:]
		if _index_chars_length(sub_chars) < 4 {
			break
		}
		split_words[_index_chars_to_str(sub_chars)] = true
	}

	// 3. 乱序索引词，对每个前缀切词，按 Unicode 编码值排序，作为一个乱序索引词
	var chaos_split_words map[string]bool
	if outOfOrder {
		chaos_split_words = make(map[string]bool)
		for split_word, _ := range split_words {
			// 将字符串转换为 rune 切片
			runes := []rune(split_word)

			// 按 Unicode 编码值对 rune 切片排序
			sort.Slice(runes, func(i, j int) bool {
				return runes[i] < runes[j]
			})

			// 将排序后的 rune 切片转换回字符串
			chaos_split_words[string(runes)] = true
		}
	}

	// 4. 掩码索引词，对每个前缀切词，除首尾各一个rune以外，中间的rune，按 1~maskOffCode 个掩码打码，作为掩码索引词
	var mask_split_words map[string]bool
	if maskCount > 0 {
		mask_split_words = make(map[string]bool)
		for split_word, _ := range split_words {
			split_chars := to_index_chars(split_word)
			n := min(len(split_chars)-1, 6) // 最多对前6个字符打码
			r := min(maskCount, n-1)
			mask_indexes := generateCombinations(n, r)
			for _, mask_index := range mask_indexes {
				mask_chars := make([]*index_char, len(split_chars))
				copy(mask_chars, split_chars)
				for _, idx := range mask_index {
					mask_chars[idx] = &index_char{CharStr: "*", CharType: split_chars[idx].CharType}
				}
				if _index_chars_length(mask_chars) > 3 {
					mask_split_words[_index_chars_to_str(mask_chars)] = true
				}
			}
		}
	}

	// 合并结果
	if outOfOrder {
		for chaos_split_word, _ := range chaos_split_words {
			split_words[chaos_split_word] = true
		}
	}
	if maskCount > 0 {
		for mask_split_word, _ := range mask_split_words {
			split_words[mask_split_word] = true
		}
	}
	results := make([]string, 0, len(split_words))
	for split_word := range split_words {
		results = append(results, split_word)
	}
	return results
}

// generateCombinations 生成从 1 到 n 中取出 r 个数字的所有组合
func generateCombinations(n int, r int) [][]int {
	var results [][]int

	// 辅助函数用于递归生成组合
	var combinations func(start int, count int, current []int)
	combinations = func(start int, count int, current []int) {
		if count == 0 {
			// 达到所需数量，保存当前组合
			combination := make([]int, len(current))
			copy(combination, current)
			results = append(results, combination)
			return
		}
		for i := start; i <= n; i++ {
			// 递归选择元素
			combinations(i+1, count-1, append(current, i))
		}
	}

	// 初始化递归
	combinations(1, r, []int{})
	return results
}

func to_index_chars(input string) []*index_char {
	chars := make([]*index_char, 0)
	words := strings.Split(input, " ")
	for _, word := range words {
		if HasHanChar(word) { // 包含汉字的词，每个字符都是一个index_character
			for _, c := range word {
				chars = append(chars, &index_char{CharStr: string(c), CharType: 0})
			}
		} else { // 没有汉字的词，整个词作为一个index_character
			chars = append(chars, &index_char{CharStr: word, CharType: 1})
		}
	}
	return chars
}

func create_index_phrase(sentence string) *index_phrase {
	chars := to_index_chars(sentence)
	return &index_phrase{index_chars: chars}
}

type IndexSentence struct {
	index_phrases []*index_phrase
}

func (is *IndexSentence) ToString() string {
	result := ""
	for _, p := range is.index_phrases {
		result += p.ToString() + "|"
	}
	return strings.TrimRight(result, "|")
}

func (is *IndexSentence) ToPinyin() string {
	result := ""
	for _, p := range is.index_phrases {
		result += p.ToPinyin() + "|"
	}
	return strings.TrimRight(result, "|")
}

func (is *IndexSentence) ToWords() []string {
	words := make([]string, 0)
	for _, p := range is.index_phrases {
		words = append(words, p.ToString())
	}
	return words
}

func (is *IndexSentence) IndexSentenceTrim(prefixes []string, suffixes []string, endingDigits int) {
	for _, p := range is.index_phrases {
		p.IndexPhraseTrim(prefixes, suffixes, endingDigits)
	}
}

func (is *IndexSentence) SplitToIndexWords(maskCount int, outOfOrder bool) []string {
	wordSet := make(map[string]bool)
	for _, p := range is.index_phrases {
		words := p.SplitToIndexWords(maskCount, outOfOrder)
		for _, w := range words {
			wordSet[w] = true
		}
	}
	result := make([]string, 0)
	for w := range wordSet {
		result = append(result, w)
	}
	return result
}

func CreateIndexSentence(words []string) *IndexSentence {
	phrases := make([]*index_phrase, 0)
	for _, word := range words {
		phrases = append(phrases, create_index_phrase(word))
	}
	return &IndexSentence{index_phrases: phrases}
}

func NewIndexSentence(sentence string) *IndexSentence {
	input := strings.ToLower(strings.TrimSpace(sentence))
	words := split_and_trim(input)
	real_words := []string{}
	current_word := ""
	for _, word := range words {
		subs := trim_bracket(word)
		for _, sub := range subs {
			cw := classify_word(sub)
			if cw == 0 || cw == 1 {
				if current_word != "" {
					real_words = append(real_words, current_word)
					current_word = ""
				}
				real_words = append(real_words, sub)
			} else {
				if current_word == "" {
					current_word = sub
				} else {
					current_word = current_word + " " + sub
				}
			}
		}
	}
	if current_word != "" {
		real_words = append(real_words, current_word)
	}
	phreses := make([]*index_phrase, 0)
	for _, word := range real_words {
		phreses = append(phreses, create_index_phrase(word))
	}
	return &IndexSentence{index_phrases: phreses}
}

func split_and_trim(input string) []string {
	runes := []rune(input)
	word_chars := []rune{}
	left_class := 0
	for i, r := range runes {
		this_class := classify_rune(r)
		if this_class == 1 { //其他字符当做空格处理，连续空格只保留一个
			if left_class > 1 {
				word_chars = append(word_chars, ' ')
			}
			left_class = this_class
		} else if this_class == 2 || this_class == 4 || this_class == 5 || this_class == 6 || this_class == 7 {
			// 数字、英文、中文、左括号、右括号，直接添加
			word_chars = append(word_chars, r)
			left_class = this_class
		} else if this_class == 3 {
			if left_class == 2 { //小数点前面是数字，继续判定
				if i < len(runes)-1 && classify_rune(runes[i+1]) == 2 {
					// 小数点后面是数字，添加小数点
					word_chars = append(word_chars, r)
					left_class = this_class
				} else {
					// 小数点后面不是数字，当做空格处理
					if left_class > 1 {
						word_chars = append(word_chars, ' ')
					}
					left_class = 1
				}
			} else {
				// 小数点前面不是数字，当做空格处理
				if left_class > 1 {
					word_chars = append(word_chars, ' ')
				}
				left_class = 1
			}
		}
	}
	return strings.Fields(string(word_chars))
}

func replace_brackets_with_space(input string) string {
	// 定义需要替换的字符
	brackets := "()（）[]【】{}<>《》"
	// 使用strings.Builder构建新字符串
	var builder strings.Builder

	// 遍历字符串，遇到括号替换为空格
	for _, r := range input {
		if strings.ContainsRune(brackets, r) {
			builder.WriteRune(' ') // 替换为一个空格
		} else {
			builder.WriteRune(r) // 保留原字符
		}
	}

	return builder.String()
}

func trim_bracket(input string) []string {
	original_len := len(input)
	input = replace_brackets_with_space(input)
	input = strings.TrimSpace(input)
	trimed_len := len(input)
	if original_len != trimed_len {
		return strings.Fields(input)
	} else {
		input = strings.ReplaceAll(input, " ", "")
		return []string{input}
	}
}

func classify_rune(r rune) int {
	// 汉字
	if unicode.Is(unicode.Han, r) {
		return 5
	}
	// 英文字母判断
	if unicode.IsLetter(r) && unicode.Is(unicode.Latin, r) {
		return 4
	}
	// 数字判断
	if unicode.IsDigit(r) {
		return 2
	}
	// 小数点判断
	if r == '.' {
		return 3
	}
	// 左括号判断
	if r == '(' || r == '[' || r == '{' || r == '（' || r == '【' || r == '｛' || r == '<' || r == '《' {
		return 6
	}
	// 右括号判断
	if r == ')' || r == ']' || r == '}' || r == '）' || r == '】' || r == '｝' || r == '>' || r == '》' {
		return 7
	}
	return 1
}

func classify_word(word string) int {
	numberic := true
	for _, r := range word {
		cr := classify_rune(r)
		if cr == 5 {
			return 0
		} else if cr != 1 && cr != 2 && cr != 3 {
			numberic = false
		}
	}
	if numberic {
		return 1
	} else {
		return 2
	}
}

func word_to_pinyin(word string) string {
	// 汉字转拼音
	args := pinyin.NewArgs()
	args.Style = pinyin.Normal // 不带声调的拼音

	result := ""
	pre_space := false
	for _, r := range word {
		if unicode.Is(unicode.Han, r) { // 判断是否为汉字
			pinyinResult := pinyin.Pinyin(string(r), args)
			if len(pinyinResult) > 0 && len(pinyinResult[0]) > 0 {
				if !pre_space {
					result += " "
				}
				result += pinyinResult[0][0] + " " // 只取第一个拼音
				pre_space = true
			}
		} else {
			result += string(r) // 非汉字部分保持原样
			pre_space = false
		}
	}
	return strings.TrimSpace(result)
}
