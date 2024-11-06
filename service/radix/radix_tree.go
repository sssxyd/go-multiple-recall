package radix

type RadixNode struct {
	ID       int
	Key      string
	Weight   int
	Children map[string]*RadixNode
}

type RadixTree struct {
	TopLevelNodes map[string]*RadixNode
}
