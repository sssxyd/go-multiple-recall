package radix

import (
	"regexp"
	"strings"
	"unicode"

	"github.com/mozillazg/go-pinyin"
)

// func classifyRune(r rune) int {
// 	// 汉字
// 	if unicode.Is(unicode.Han, r) {
// 		return 5
// 	}
// 	// 英文字母判断
// 	if unicode.IsLetter(r) && unicode.Is(unicode.Latin, r) {
// 		return 4
// 	}
// 	// 数字判断
// 	if unicode.IsDigit(r) {
// 		return 2
// 	}
// 	// 小数点判断
// 	if r == '.' {
// 		return 3
// 	}
// 	// 左括号判断
// 	if r == '(' || r == '[' || r == '{' || r == '（' || r == '【' || r == '｛' || r == '<' || r == '《' {
// 		return 6
// 	}
// 	// 右括号判断
// 	if r == ')' || r == ']' || r == '}' || r == '）' || r == '】' || r == '｝' || r == '>' || r == '》' {
// 		return 7
// 	}
// 	return 1
// }

// 0: 汉字/混合词 1: 纯数字 2: 英文/英文数字混合
// func classifyWord(word string) int {
// 	numberic := true
// 	for _, r := range word {
// 		cr := classifyRune(r)
// 		if cr == 5 {
// 			return 0
// 		} else if cr != 1 && cr != 2 && cr != 3 {
// 			numberic = false
// 		}
// 	}
// 	if numberic {
// 		return 1
// 	} else {
// 		return 2
// 	}
// }

// func trimAndSplit(input string) []string {
// 	runes := []rune(input)
// 	word_chars := []rune{}
// 	left_class := 0
// 	for i, r := range runes {
// 		this_class := classifyRune(r)
// 		if this_class == 1 { //其他字符当做空格处理，连续空格只保留一个
// 			if left_class > 1 {
// 				word_chars = append(word_chars, ' ')
// 			}
// 			left_class = this_class
// 		} else if this_class == 2 || this_class == 4 || this_class == 5 || this_class == 6 || this_class == 7 {
// 			// 数字、英文、中文、左括号、右括号，直接添加
// 			word_chars = append(word_chars, r)
// 			left_class = this_class
// 		} else if this_class == 3 {
// 			if left_class == 2 { //小数点前面是数字，继续判定
// 				if i < len(runes)-1 && classifyRune(runes[i+1]) == 2 {
// 					// 小数点后面是数字，添加小数点
// 					word_chars = append(word_chars, r)
// 					left_class = this_class
// 				} else {
// 					// 小数点后面不是数字，当做空格处理
// 					if left_class > 1 {
// 						word_chars = append(word_chars, ' ')
// 					}
// 					left_class = 1
// 				}
// 			} else {
// 				// 小数点前面不是数字，当做空格处理
// 				if left_class > 1 {
// 					word_chars = append(word_chars, ' ')
// 				}
// 				left_class = 1
// 			}
// 		}
// 	}
// 	return strings.Fields(string(word_chars))
// }

// func replaceBracketsWithSpace(input string) string {
// 	// 定义需要替换的字符
// 	brackets := "()（）[]【】{}<>《》"
// 	// 使用strings.Builder构建新字符串
// 	var builder strings.Builder

// 	// 遍历字符串，遇到括号替换为空格
// 	for _, r := range input {
// 		if strings.ContainsRune(brackets, r) {
// 			builder.WriteRune(' ') // 替换为一个空格
// 		} else {
// 			builder.WriteRune(r) // 保留原字符
// 		}
// 	}

// 	return builder.String()
// }

// func trimBracket(input string) []string {
// 	original_len := len(input)
// 	input = replaceBracketsWithSpace(input)
// 	input = strings.Trim(input, " \t\n\r\v\f") // 去掉开头和结尾的空白字符
// 	trimed_len := len(input)
// 	if original_len != trimed_len {
// 		return strings.Fields(input)
// 	} else {
// 		input = strings.ReplaceAll(input, " ", "")
// 		return []string{input}
// 	}
// }

func PinyinOfWord(word string) (py string, ok bool) {
	// 汉字转拼音
	args := pinyin.NewArgs()
	args.Style = pinyin.Normal // 不带声调的拼音

	pre_space := false
	for _, r := range word {
		if unicode.Is(unicode.Han, r) { // 判断是否为汉字
			pinyinResult := pinyin.Pinyin(string(r), args)
			if len(pinyinResult) > 0 && len(pinyinResult[0]) > 0 {
				if !pre_space {
					py += " "
				}
				py += pinyinResult[0][0] + " " // 只取第一个拼音
				pre_space = true
			} else {
				ok = false
			}
		} else {
			py += string(r) // 非汉字部分保持原样
			pre_space = false
		}
	}
	return strings.TrimSpace(py), ok
}

/**
 * 判断字符串中是否包含汉字
 */
func HasHanChar(word string) bool {
	for _, r := range word {
		if unicode.Is(unicode.Han, r) {
			return true
		}
	}
	return false
}

func IsPureHanWord(word string) bool {
	for _, r := range word {
		if !unicode.Is(unicode.Han, r) {
			return false
		}
	}
	return true
}

func IsNumeric(s string) bool {
	// 正则匹配整数或小数
	regex := regexp.MustCompile(`^\d+(\.\d+)?$`)
	return regex.MatchString(s)
}

func StartWithHanChar(word string) bool {
	runes := []rune(word)
	if len(runes) == 0 {
		return false
	}
	return unicode.Is(unicode.Han, runes[0])
}

func EndWithHanChar(word string) bool {
	runes := []rune(word)
	if len(runes) == 0 {
		return false
	}
	return unicode.Is(unicode.Han, runes[len(runes)-1])
}

// func IndexLength(word string) int {
// 	count := 0
// 	for _, r := range word {
// 		if unicode.Is(unicode.Han, r) {
// 			count += 2
// 		} else {
// 			count++
// 		}
// 	}
// 	return count
// }

// /**
//  * 分析词条，返回词条中的字符、拼音、拼音缩写
//  */
// func AnalyzeDictWord(input string) (word_chars string, word_pinyin string, word_pinyin_abbr string) {
// 	input = strings.Trim(input, " \t\n\r\v\f")
// 	input = strings.ToLower(input)
// 	words := trimAndSplit(input)
// 	real_words := []string{}
// 	current_word := ""
// 	for _, word := range words {
// 		subs := trimBracket(word)
// 		for _, sub := range subs {
// 			cw := classifyWord(sub)
// 			if cw == 0 || cw == 1 {
// 				if current_word != "" {
// 					real_words = append(real_words, current_word)
// 					current_word = ""
// 				}
// 				real_words = append(real_words, sub)
// 			} else {
// 				if current_word == "" {
// 					current_word = sub
// 				} else {
// 					current_word = current_word + " " + sub
// 				}
// 			}
// 		}
// 	}
// 	if current_word != "" {
// 		real_words = append(real_words, current_word)
// 	}
// 	word_chars = strings.Join(real_words, "|")
// 	pinyin_words := []string{}
// 	abbr_words := []string{}
// 	for _, word := range real_words {
// 		if HasHanChar(word) {
// 			pinyin, pinyin_abbr := pinyinOfWord(word)
// 			pinyin_words = append(pinyin_words, pinyin)
// 			abbr_words = append(abbr_words, pinyin_abbr)
// 		} else {
// 			pinyin_words = append(pinyin_words, word)
// 			abbr_words = append(abbr_words, word)
// 		}
// 	}
// 	word_pinyin = strings.Join(pinyin_words, "|")
// 	word_pinyin_abbr = strings.Join(abbr_words, "|")
// 	return word_chars, word_pinyin, word_pinyin_abbr
// }

// func TrieSplitWord(word string, outOfOrder bool, maskOffCode int) []string {
// 	if IsNumeric(word) {
// 		return []string{word}
// 	}

// 	runes := []rune(word)
// 	word_len := len(runes)
// 	split_words := make(map[string]bool)

// 	// 1. 自身作为一个索引词
// 	split_words[word] = true

// 	// 2. 前缀切词，每少一个rune，作为一个索引词，直至切出的索引词长度小于4
// 	for i := 1; i < word_len-2; i++ {
// 		sub_word := string(runes[i:])
// 		if IndexLength(sub_word) < 4 {
// 			break
// 		}
// 		split_words[sub_word] = true
// 	}

// 	// 3. 乱序索引词，对每个前缀切词，按 Unicode 编码值排序，作为一个乱序索引词
// 	var chaos_split_words map[string]bool
// 	if outOfOrder && IsPureHanWord(word) { // 只有全汉字词才支持乱序
// 		chaos_split_words = make(map[string]bool)
// 		for split_word, _ := range split_words {
// 			// 将字符串转换为 rune 切片
// 			runes := []rune(split_word)

// 			// 按 Unicode 编码值对 rune 切片排序
// 			sort.Slice(runes, func(i, j int) bool {
// 				return runes[i] < runes[j]
// 			})

// 			// 将排序后的 rune 切片转换回字符串
// 			chaos_split_words[string(runes)] = true
// 		}
// 	}

// 	// 4. 掩码索引词，对每个前缀切词，除首尾各一个rune以外，中间的rune，按 1~maskOffCode 个掩码打码，作为掩码索引词
// 	var mask_split_words map[string]bool
// 	if maskOffCode > 0 {
// 		mask_split_words = make(map[string]bool)
// 		for split_word, _ := range split_words {
// 			runes := []rune(split_word)
// 			if len(runes) > 3 {
// 				first_rule := runes[0]
// 				mask_source := string(runes[1:])
// 				mask_sub_words := GenerateMaskCombinations(mask_source, maskOffCode)
// 				for _, mask_sub_word := range mask_sub_words {
// 					mask_word := string(first_rule) + mask_sub_word
// 					mask_split_words[mask_word] = true
// 				}
// 			}
// 		}
// 	}

// 	// 合并结果
// 	if outOfOrder {
// 		for chaos_split_word, _ := range chaos_split_words {
// 			split_words[chaos_split_word] = true
// 		}
// 	}
// 	if maskOffCode > 0 {
// 		for mask_split_word, _ := range mask_split_words {
// 			split_words[mask_split_word] = true
// 		}
// 	}
// 	results := make([]string, 0, len(split_words))
// 	for split_word, _ := range split_words {
// 		results = append(results, split_word)
// 	}
// 	return results
// }

// // generateMaskCombinations 生成指定数量的掩码组合
// func GenerateMaskCombinations(s string, maskCount int) []string {
// 	var results []string
// 	chars := []rune(s)
// 	indices := make([]int, len(chars))
// 	for i := range len(chars) {
// 		indices[i] = i
// 	}

// 	// 生成指定数量的掩码组合
// 	combinations := combinations(indices, maskCount)
// 	for _, combo := range combinations {
// 		runes := []rune(s)
// 		for _, idx := range combo {
// 			if idx >= len(runes) {
// 				fmt.Println("Fuck!")
// 			}
// 			runes[idx] = '*' // 将指定位置字符替换为 '*'
// 		}
// 		results = append(results, string(runes))
// 	}

// 	return results
// }

// // combinations 生成从 n 个元素中选择 k 个元素的所有组合
// func combinations(arr []int, k int) [][]int {
// 	var res [][]int

// 	var helper func(int, int, []int)
// 	helper = func(offset, k int, currentComb []int) {
// 		if k == 0 {
// 			// 将当前组合添加到 res 中
// 			comb := make([]int, len(currentComb))
// 			copy(comb, currentComb)
// 			res = append(res, comb)
// 			return
// 		}
// 		for i := offset; i <= len(arr)-k; i++ {
// 			helper(i+1, k-1, append(currentComb, arr[i]))
// 		}
// 	}

// 	// 调用 helper，传递初始组合为空的切片
// 	helper(0, k, []int{})
// 	return res
// }

// func mergeAndRemoveDuplicates(slice1, slice2 []int) []int {
// 	// 创建一个 map 来存储唯一元素
// 	uniqueMap := make(map[int]bool)

// 	// 添加第一个切片的元素到 map 中
// 	for _, v := range slice1 {
// 		uniqueMap[v] = true
// 	}

// 	// 添加第二个切片的元素到 map 中
// 	for _, v := range slice2 {
// 		uniqueMap[v] = true
// 	}

// 	// 将 map 中的键转换回切片
// 	result := make([]int, 0, len(uniqueMap))
// 	for k := range uniqueMap {
// 		result = append(result, k)
// 	}

// 	return result
// }
